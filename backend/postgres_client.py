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

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row

from config.settings import POSTGRES_DSN, SERVER_EMA_LENGTHS, TIMEFRAMES

ONE_MINUTE = timedelta(minutes=1)

# candle_indicators' value columns (storage/ddl/indicators.sql).
INDICATOR_COLUMNS = [f"ema_{period}" for period in SERVER_EMA_LENGTHS]

# Rolls candles_1m up into `bucket`-sized candles, newest `limit` first.
#
# The inner query takes only as many 1m rows as `limit` buckets can hold
# (limit x minutes-per-bucket), so the scan stays bounded instead of
# aggregating a symbol's whole history. That row cap can cut the oldest
# bucket short — but a bucket holds at most minutes-per-bucket rows, so
# whenever the cap bites the rows span *more* than `limit` buckets, and the
# truncated one is always the oldest, which the outer LIMIT drops. Every
# returned bucket is complete (apart from the in-progress newest one).
#
# Buckets are aligned to the Unix epoch — floor(t / bucket) * bucket — the
# same alignment frontend/src/lib/timeframes.js uses to roll live 1m
# candles up, and streaming/indicator_engine.py buckets indicators by, so
# live and historical bars for a bucket share a start time and the
# indicator join below is a plain equality.
_HISTORY_SQL = f"""
    SELECT candle.*, {", ".join(f"indicator.{c}" for c in INDICATOR_COLUMNS)}
    FROM (
        SELECT
            symbol,
            bucket AS window_start,
            bucket + %(bucket)s AS window_end,
            (array_agg(open ORDER BY window_start))[1] AS open,
            max(high) AS high,
            min(low) AS low,
            (array_agg(close ORDER BY window_start DESC))[1] AS close,
            sum(volume) AS volume,
            sum(trade_count) AS trade_count
        FROM (
            SELECT *, date_bin(%(bucket)s, window_start, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket
            FROM candles_1m
            WHERE symbol = %(symbol)s
              AND window_start < COALESCE(%(before)s::timestamptz, 'infinity')
            ORDER BY window_start DESC
            LIMIT %(row_limit)s
        ) recent
        GROUP BY symbol, bucket
        ORDER BY bucket DESC
        LIMIT %(limit)s
    ) candle
    LEFT JOIN candle_indicators indicator
        ON indicator.symbol = candle.symbol
       AND indicator.timeframe = %(timeframe)s
       AND indicator.window_start = candle.window_start
    ORDER BY candle.window_start DESC
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
