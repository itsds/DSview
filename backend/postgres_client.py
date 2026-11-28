"""
postgres_client.py

Read-only Postgres access for REST endpoints — the query-side counterpart
to streaming/postgres_sink.py's write-side CandlePostgresSink. Kept as
its own module so routers/candles.py never touches `psycopg` directly,
mirroring the same boundary pattern redis_client.py uses for Redis.

Only 1-minute candles are stored (candles_1m); every bigger timeframe is
rolled up from them at query time with date_bin() rather than kept as
extra tables — see _HISTORY_SQL. Indicators, which Spark computes per
timeframe (streaming/jobs/indicators.py), are stored per timeframe in
candle_indicators and joined onto each rolled-up candle by its start.

The footprint (FootprintReader) works the same way: footprint_1m holds
$1 levels per minute, and each request merges them into its row size and
timeframe (_FOOTPRINT_SQL), or sums a time range into a volume profile
(_PROFILE_SQL). Backtest results (BacktestReader, Phase 4b) are read as
backtest/backtest_store.py wrote them, the equity curve thinned for the
chart (_EQUITY_SQL). The long/short ratios (FuturesRatioReader, Phase 4c)
are stored per 5-minute period and rolled up to each timeframe like
candles (_FUTURES_RATIOS_SQL).

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from config.settings import FOOTPRINT_PRICE_STEP, POSTGRES_DSN, SERVER_EMA_LENGTHS, TIMEFRAMES

ONE_MINUTE = timedelta(minutes=1)

# candle_indicators' value columns (storage/ddl/indicators.sql).
INDICATOR_COLUMNS = [f"ema_{period}" for period in SERVER_EMA_LENGTHS]

# Rolls candles_1m up into `bucket`-sized candles, newest `limit` first.
#
# `recent` takes only as many 1m rows as `limit` buckets can hold (limit x
# minutes-per-bucket), so the scan stays bounded instead of aggregating a
# symbol's whole history. That row cap can cut the oldest bucket short —
# but a bucket holds at most minutes-per-bucket rows, so whenever the cap
# bites the rows span *more* than `limit` buckets, and the truncated one is
# always the oldest, which `buckets`' LIMIT drops. Every returned bucket is
# complete (apart from the in-progress newest one).
#
# Buckets are aligned to the Unix epoch — floor(t / bucket) * bucket — the
# same alignment frontend/src/lib/timeframes.js uses to roll live 1m
# candles up, and streaming/indicator_engine.py buckets indicators by, so
# live and historical bars for a bucket share a start time and the
# indicator join below is a plain equality.
#
# Open and close come from each bucket's first and last minute, looked up
# by primary key, rather than from an ordered aggregate: with a year of
# backfilled history a page of 1D bars rolls up 432,000 rows, and
# array_agg(... ORDER BY ...) sorted them all (on disk, past work_mem),
# taking ~0.9 s. A plain hash aggregate plus 2 x `limit` index lookups
# takes ~0.2 s.
_HISTORY_SQL = f"""
    WITH recent AS (
        SELECT window_start, high, low, volume, trade_count,
               date_bin(%(bucket)s, window_start, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket
        FROM candles_1m
        WHERE symbol = %(symbol)s
          AND window_start < COALESCE(%(before)s::timestamptz, 'infinity')
        ORDER BY window_start DESC
        LIMIT %(row_limit)s
    ), buckets AS (
        SELECT
            bucket,
            min(window_start) AS first_minute,
            max(window_start) AS last_minute,
            max(high) AS high,
            min(low) AS low,
            sum(volume) AS volume,
            sum(trade_count) AS trade_count
        FROM recent
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT %(limit)s
    )
    SELECT
        first.symbol,
        buckets.bucket AS window_start,
        buckets.bucket + %(bucket)s AS window_end,
        first.open,
        buckets.high,
        buckets.low,
        last.close,
        buckets.volume,
        buckets.trade_count,
        {", ".join(f"indicator.{c}" for c in INDICATOR_COLUMNS)}
    FROM buckets
    JOIN candles_1m first ON first.symbol = %(symbol)s AND first.window_start = buckets.first_minute
    JOIN candles_1m last ON last.symbol = %(symbol)s AND last.window_start = buckets.last_minute
    LEFT JOIN candle_indicators indicator
        ON indicator.symbol = %(symbol)s
       AND indicator.timeframe = %(timeframe)s
       AND indicator.window_start = buckets.bucket
    ORDER BY buckets.bucket DESC
"""


class CandleHistoryReader:
    """Reads candles for a symbol from the candles_1m serving table, rolled up to any timeframe, with their indicators."""

    # TODO(@DS): a connection per request is fine at dev request volumes;
    # move to a pool (e.g. psycopg_pool) if this ever sees real traffic.
    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def get_candles(
        self,
        symbol: str,
        limit: int,
        timeframe: str = "1m",
        before: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Return up to `limit` candles of `timeframe` (a config.settings.TIMEFRAMES id) for `symbol`, oldest first.

        With `before`, only candles whose window starts before it — pass the
        oldest window_start already loaded to page backwards. Each candle has
        an `indicators` object, {"ema_20": value or None, ...}: None where
        Spark hasn't computed the bar (or the EMA is still seeding).
        """
        bucket = TIMEFRAMES[timeframe]
        rows = self._conn.execute(
            _HISTORY_SQL,
            {
                "symbol": symbol,
                "bucket": bucket,
                "timeframe": timeframe,
                "before": before,
                "limit": limit,
                "row_limit": limit * (bucket // ONE_MINUTE),
            },
        ).fetchall()
        for row in rows:
            row["indicators"] = {column: row.pop(column) for column in INDICATOR_COLUMNS}
        return list(reversed(rows))  # query is DESC to LIMIT to the *latest* rows; charts want ascending

    def close(self) -> None:
        self._conn.close()


_EPOCH = "TIMESTAMPTZ '1970-01-01 00:00:00+00'"

# A bar's footprint: footprint_1m's minutes rolled up into `bucket`-sized
# bars (epoch-aligned, as _HISTORY_SQL's candles), and its $1 levels into
# `row_size`-wide rows, each named by its lower bound: floor(price / row) *
# row. Grouped on the numbers, then formatted, so rows sort by price, not
# as text; trim_scale() drops NUMERIC(38, 18)'s trailing zeros
# ("1.25", not "1.250000000000000000"). The primary key's range scan reads
# only the requested minutes.
_FOOTPRINT_SQL = f"""
    SELECT bar, trim_scale(level)::text, trim_scale(buy)::text, trim_scale(sell)::text
    FROM (
        SELECT
            date_bin(%(bucket)s, window_start, {_EPOCH}) AS bar,
            floor(price / %(row_size)s) * %(row_size)s AS level,
            sum(buy_volume) AS buy,
            sum(sell_volume) AS sell
        FROM footprint_1m
        WHERE symbol = %(symbol)s AND window_start >= %(start)s AND window_start < %(end)s
        GROUP BY bar, level
    ) cells
    ORDER BY bar, level
"""

# A volume profile: every level traded in [start, end), summed over the
# range, in rows of `row_size` — or, without one, of whatever multiple of
# the step splits the range's traded prices into about `rows` rows
# (TradingView's "number of rows" layout). One scan either way: the $1
# levels are summed first, the row size picked from their spread, and the
# levels merged into it.
_PROFILE_SQL = """
    WITH levels AS (
        SELECT price, sum(buy_volume) AS buy, sum(sell_volume) AS sell
        FROM footprint_1m
        WHERE symbol = %(symbol)s AND window_start >= %(start)s AND window_start < %(end)s
        GROUP BY price
    ), size AS (
        SELECT COALESCE(
            %(row_size)s::numeric,
            greatest(%(step)s::numeric, ceil((max(price) - min(price) + %(step)s) / %(rows)s / %(step)s) * %(step)s)
        ) AS row_size
        FROM levels
    )
    SELECT trim_scale(size.row_size)::text, trim_scale(level)::text, trim_scale(buy)::text, trim_scale(sell)::text
    FROM (
        SELECT floor(price / size.row_size) * size.row_size AS level, sum(buy) AS buy, sum(sell) AS sell
        FROM levels, size
        GROUP BY level
    ) merged, size
    ORDER BY level
"""

# Which minutes of [start, end) have any footprint at all: the backfill
# covers 30 days by default, so a profile over a longer range is partial.
_COVERAGE_SQL = """
    SELECT min(window_start), max(window_start)
    FROM footprint_1m
    WHERE symbol = %(symbol)s AND window_start >= %(start)s AND window_start < %(end)s
"""


class FootprintReader:
    """Reads footprint_1m: bars at a timeframe and row size, and volume profiles over a time range."""

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def get_bars(
        self, symbol: str, timeframe: str, start: datetime, end: datetime, row_size: Decimal
    ) -> list[dict[str, Any]]:
        """
        The footprint of every `timeframe` bar starting in [start, end)
        (both aligned to bar starts by the caller), oldest first, as
        {"window_start", "levels": [[price, buy_volume, sell_volume], ...]}
        with prices ascending and numbers as plain decimal strings. A bar
        without trades isn't listed.
        """
        rows = self._conn.execute(
            _FOOTPRINT_SQL,
            {"symbol": symbol, "bucket": TIMEFRAMES[timeframe], "start": start, "end": end, "row_size": row_size},
        ).fetchall()
        bars: list[dict[str, Any]] = []
        for bar, price, buy, sell in rows:
            if not bars or bars[-1]["window_start"] != bar:
                bars.append({"window_start": bar, "levels": []})
            bars[-1]["levels"].append([price, buy, sell])
        return bars

    def get_profile(
        self, symbol: str, start: datetime, end: datetime, row_size: Decimal | None, rows: int
    ) -> dict[str, Any]:
        """
        The volume profile of [start, end): {"row_size", "first_minute",
        "last_minute", "levels": [[price, buy_volume, sell_volume], ...]},
        prices ascending. `row_size` fixes the rows' width; None picks one
        from the range's prices that makes about `rows` rows. first_minute
        and last_minute are the oldest and newest minute in the range with
        any footprint (None, and no levels, if there's none).
        """
        params = {
            "symbol": symbol, "start": start, "end": end, "row_size": row_size, "rows": rows, "step": FOOTPRINT_PRICE_STEP,
        }
        first, last = self._conn.execute(_COVERAGE_SQL, params).fetchone()
        levels = self._conn.execute(_PROFILE_SQL, params).fetchall() if first is not None else []
        return {
            "row_size": levels[0][0] if levels else (None if row_size is None else str(row_size)),
            "first_minute": first,
            "last_minute": last,
            "levels": [[price, buy, sell] for _, price, buy, sell in levels],
        }

    def close(self) -> None:
        self._conn.close()


# Backtest results (Phase 4b; storage/ddl/backtests.sql, written by
# backtest/backtest_store.py). Live's schema only: every reader below takes
# live's DSN. Money arrives as plain decimal strings (trim_scale: the
# runs keep 28 significant digits), times as bar starts.
_RUN_COLUMNS = """
    run_id, created_at, symbol, strategy, params, timeframe, first_bar, last_bar,
    trim_scale(initial_capital)::text AS initial_capital, trim_scale(fee_rate)::text AS fee_rate,
    stats, spark_check, same_as_run
"""

_TRADES_SQL = """
    SELECT trade_no, side, entry_time, trim_scale(entry_price)::text AS entry_price,
           exit_time, trim_scale(exit_price)::text AS exit_price, trim_scale(quantity)::text AS quantity,
           trim_scale(fees)::text AS fees, trim_scale(pnl)::text AS pnl,
           trim_scale(return_pct)::text AS return_pct, bars, closed_by_end
    FROM backtest_trades
    WHERE run_id = %(run_id)s
    ORDER BY trade_no
"""

# The equity curve, thinned to about `max_points` points when it has more
# (a year of 1m bars is 525,600): the points split into max_points / 2
# runs of consecutive bars, and each run keeps its lowest and highest
# point, so every trough and peak the full curve has (the max drawdown
# included) survives. The first and last points always stay.
_EQUITY_SQL = """
    WITH points AS (
        SELECT window_start, equity,
               row_number() OVER (ORDER BY window_start) - 1 AS i,
               count(*) OVER () AS n
        FROM backtest_equity
        WHERE run_id = %(run_id)s
    ), ranked AS (
        SELECT window_start, equity, i, n,
               row_number() OVER (PARTITION BY i * %(runs)s / n ORDER BY equity, window_start) AS low,
               row_number() OVER (PARTITION BY i * %(runs)s / n ORDER BY equity DESC, window_start) AS high
        FROM points
    )
    SELECT window_start, trim_scale(equity)::text AS equity, n
    FROM ranked
    WHERE n <= %(max_points)s OR low = 1 OR high = 1 OR i = 0 OR i = n - 1
    ORDER BY window_start
"""


class BacktestReader:
    """Reads backtest runs, their trades and equity curves."""

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)

    def list_runs(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        """`symbol`'s newest `limit` runs, newest first, each with its stats."""
        return self._conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM backtest_runs WHERE symbol = %s ORDER BY run_id DESC LIMIT %s",
            (symbol, limit),
        ).fetchall()

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        """One run with its trades (oldest first), or None if there's no such run."""
        run = self._conn.execute(f"SELECT {_RUN_COLUMNS} FROM backtest_runs WHERE run_id = %s", (run_id,)).fetchone()
        if run is not None:
            run["trades"] = self._conn.execute(_TRADES_SQL, {"run_id": run_id}).fetchall()
        return run

    def run_exists(self, run_id: int) -> bool:
        return self._conn.execute("SELECT 1 FROM backtest_runs WHERE run_id = %s", (run_id,)).fetchone() is not None

    def get_equity(self, run_id: int, max_points: int) -> dict[str, Any]:
        """{"points": [[bar start, equity], ...] oldest first, "total": points in the full curve}."""
        rows = self._conn.execute(
            _EQUITY_SQL, {"run_id": run_id, "runs": max(1, max_points // 2), "max_points": max_points}
        ).fetchall()
        return {
            "points": [[row["window_start"], row["equity"]] for row in rows],
            "total": rows[0]["n"] if rows else 0,
        }

    def close(self) -> None:
        self._conn.close()


# The long/short ratios (Phase 4c; storage/ddl/futures_ratios.sql, written
# by streaming/jobs/futures_ratios.py). Live's schema only.
#
# futures_ratios' 5-minute periods rolled up into `bucket`-sized bars,
# epoch-aligned like the candles (_HISTORY_SQL), so a bar of ratios and its
# candle share a start. The account and position ratios are snapshots:
# each bar takes its last period, the ratio as of the bar's close, as a
# candle takes its last minute's close. Taker volume adds up: a bar's
# ratio is its buy volume over its sell volume, rounded to Binance's four
# decimals, except where the bar is a single period, which keeps
# Binance's own ratio (on 5m, every bar). The primary key's range scan
# reads only the requested periods.
_FUTURES_RATIOS_SQL = f"""
    WITH periods AS (
        SELECT date_bin(%(bucket)s, period_start, {_EPOCH}) AS bar, period_start, kind, ratio, long_value, short_value
        FROM futures_ratios
        WHERE symbol = %(symbol)s AND period_start >= %(start)s AND period_start < %(end)s
    ), snapshots AS (
        SELECT DISTINCT ON (bar, kind) bar, kind, ratio, long_value, short_value
        FROM periods
        WHERE kind <> 'taker_volume'
        ORDER BY bar, kind, period_start DESC
    ), taker AS (
        SELECT bar, kind,
               CASE WHEN count(*) = 1 THEN max(ratio)
                    WHEN sum(short_value) > 0 THEN round(sum(long_value) / sum(short_value), 4)
               END AS ratio,
               sum(long_value) AS long_value, sum(short_value) AS short_value
        FROM periods
        WHERE kind = 'taker_volume'
        GROUP BY bar, kind
    )
    SELECT bar, kind, trim_scale(ratio)::text, trim_scale(long_value)::text, trim_scale(short_value)::text
    FROM (SELECT * FROM snapshots UNION ALL SELECT * FROM taker) rolled_up
    ORDER BY bar, kind
"""


class FuturesRatioReader:
    """Reads futures_ratios, rolled up to a timeframe."""

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def get_bars(self, symbol: str, bucket: timedelta, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """
        Every `bucket`-sized bar starting in [start, end) (aligned to bar
        starts by the caller) with any ratio, oldest first, as
        {"window_start", "ratios": {kind: {"ratio", "long", "short"}}}:
        numbers as plain decimal strings, a ratio missing from a bar it has
        no period in. A taker ratio is None where the bar sold nothing.
        """
        rows = self._conn.execute(
            _FUTURES_RATIOS_SQL, {"symbol": symbol, "bucket": bucket, "start": start, "end": end}
        ).fetchall()
        bars: list[dict[str, Any]] = []
        for bar, kind, ratio, long, short in rows:
            if not bars or bars[-1]["window_start"] != bar:
                bars.append({"window_start": bar, "ratios": {}})
            bars[-1]["ratios"][kind] = {"ratio": ratio, "long": long, "short": short}
        return bars

    def newest_period(self, symbol: str) -> datetime | None:
        """The start of `symbol`'s newest stored period, if it has any."""
        return self._conn.execute(
            "SELECT max(period_start) FROM futures_ratios WHERE symbol = %s", (symbol,)
        ).fetchone()[0]

    def close(self) -> None:
        self._conn.close()
