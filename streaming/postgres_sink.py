"""
postgres_sink.py

Postgres serving-layer writers: CandlePostgresSink for the Gold-tier
OHLCV candles produced by candle_aggregator.py, FootprintPostgresSink for
the footprint levels its footprint query produces, IndicatorPostgresSink
for the indicators jobs/indicators.py computes, FuturesRatioPostgresSink
for the long/short ratios jobs/futures_ratios.py consumes — plus read_minute_closes(),
which that job warms its indicator state up from, and
read_candle_revision(), which tells it when a backfill changed that
history. The one place streaming/ touches psycopg.

The backend-queryable counterpart to redis_sink.py: Redis serves "what's
happening right now" (one key, TTL'd, overwritten); Postgres serves
"history" (one row per symbol/window, upserted as a window's values
refine, never deleted) — the REST endpoint the frontend will eventually
backfill from reads this table, not Redis.

Upserting on every foreachBatch call — rather than writing only once a
window is finalized — is deliberate: Structured Streaming's update mode
never signals "this is the final update for this window." Repeated
upserts naturally converge to the correct final values once the
watermark closes a window and no more updates for it arrive, so no
separate "is this closed?" bookkeeping is needed.

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Tuple

import psycopg

from config.settings import POSTGRES_DSN

from indicator_engine import INDICATOR_COLUMNS

_UPSERT_SQL = """
    INSERT INTO candles_1m
        (symbol, window_start, window_end, open, high, low, close, volume, trade_count)
    VALUES
        (%(symbol)s, %(window_start)s, %(window_end)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(trade_count)s)
    ON CONFLICT (symbol, window_start) DO UPDATE SET
        window_end = EXCLUDED.window_end,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count
"""


class CandlePostgresSink:
    """Upserts each candle update into the candles_1m serving table."""

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def write_candle(self, candle: Mapping[str, Any]) -> None:
        """Upsert one candle row, keyed on (symbol, window_start)."""
        self._conn.execute(_UPSERT_SQL, dict(candle))

    def close(self) -> None:
        self._conn.close()


_FOOTPRINT_UPSERT_SQL = """
    INSERT INTO footprint_1m (symbol, window_start, price, buy_volume, sell_volume)
    VALUES (%(symbol)s, %(window_start)s, %(price)s, %(buy_volume)s, %(sell_volume)s)
    ON CONFLICT (symbol, window_start, price) DO UPDATE SET
        buy_volume = EXCLUDED.buy_volume,
        sell_volume = EXCLUDED.sell_volume
"""


class FootprintPostgresSink:
    """
    Upserts footprint levels into footprint_1m (storage/ddl/footprint.sql),
    a micro-batch at a time.

    Update mode sends each level a batch changed with its window's running
    totals, so the upsert replaces both volumes, and repeated upserts
    converge on the minute's final values, as CandlePostgresSink's do. One
    transaction per batch, so a reader never sees half a batch's levels.
    """

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def write_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Upsert rows of symbol, window_start (aware UTC), price, buy_volume and sell_volume."""
        rows = list(rows)
        if not rows:
            return
        with self._conn.transaction(), self._conn.cursor() as cursor:
            cursor.executemany(_FOOTPRINT_UPSERT_SQL, rows)

    def close(self) -> None:
        self._conn.close()


_INDICATOR_UPSERT_SQL = f"""
    INSERT INTO candle_indicators (symbol, timeframe, window_start, {", ".join(INDICATOR_COLUMNS)})
    VALUES (%(symbol)s, %(timeframe)s, %(window_start)s, {", ".join(f"%({c})s" for c in INDICATOR_COLUMNS)})
    ON CONFLICT (symbol, timeframe, window_start) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in INDICATOR_COLUMNS)}
"""


class IndicatorPostgresSink:
    """
    Upserts indicator rows into candle_indicators, a micro-batch at a time.

    Repeated upserts converge on a bar's final values, as CandlePostgresSink's
    do. One transaction per batch: a warm-up batch can carry every bar of
    the stored history at once, and executemany() pipelines them.
    """

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def write_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Upsert rows of symbol, timeframe, window_start (aware UTC) and one value per INDICATOR_COLUMNS."""
        with self._conn.transaction(), self._conn.cursor() as cursor:
            cursor.executemany(_INDICATOR_UPSERT_SQL, list(rows))

    def close(self) -> None:
        self._conn.close()


# Rewrites a row only when a value differs (NUMERIC compares by value, so
# "1.5000" from Binance equals the stored 1.5), and returns the rows it
# inserted or changed: a refetch of unchanged history changes nothing.
_FUTURES_RATIO_UPSERT_SQL = """
    INSERT INTO futures_ratios (symbol, period_start, kind, ratio, long_value, short_value)
    VALUES (%(symbol)s, %(period_start)s, %(kind)s, %(ratio)s, %(long_value)s, %(short_value)s)
    ON CONFLICT (symbol, period_start, kind) DO UPDATE SET
        ratio = EXCLUDED.ratio,
        long_value = EXCLUDED.long_value,
        short_value = EXCLUDED.short_value
    WHERE (futures_ratios.ratio, futures_ratios.long_value, futures_ratios.short_value)
          IS DISTINCT FROM (EXCLUDED.ratio, EXCLUDED.long_value, EXCLUDED.short_value)
    RETURNING symbol, period_start
"""


class FuturesRatioPostgresSink:
    """
    Upserts Binance Futures' long/short ratios into futures_ratios
    (storage/ddl/futures_ratios.sql), a consumer batch at a time, in one
    transaction.
    """

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def write_rows(self, rows: Iterable[Mapping[str, Any]]) -> List[Tuple[str, datetime]]:
        """
        Upsert rows of symbol, period_start (aware UTC), kind, ratio,
        long_value and short_value; (symbol, period_start) of every row that
        was new or differed.
        """
        rows = list(rows)
        if not rows:
            return []
        changed: List[Tuple[str, datetime]] = []
        with self._conn.transaction(), self._conn.cursor() as cursor:
            cursor.executemany(_FUTURES_RATIO_UPSERT_SQL, rows, returning=True)
            while True:
                changed.extend(cursor.fetchall())
                if not cursor.nextset():
                    break
        return changed

    def close(self) -> None:
        self._conn.close()


def read_minute_closes(symbol: str, before: int, dsn: str = POSTGRES_DSN) -> List[Tuple[int, str]]:
    """
    Every stored 1m candle of `symbol` starting before `before` (Unix
    seconds), oldest first, as (Unix seconds, close): what
    indicator_state.py warms a symbol's indicators up from the first time
    it sees it. The close stays text, exactly as stored.
    """
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT window_start, close::text AS close
            FROM candles_1m
            WHERE symbol = %s AND window_start < %s
            ORDER BY window_start
            """,
            (symbol, datetime.fromtimestamp(before, timezone.utc)),
        ).fetchall()
    return [(int(window_start.timestamp()), close) for window_start, close in rows]


def read_candle_revision(symbol: str, dsn: str = POSTGRES_DSN) -> int:
    """
    `symbol`'s revision in candle_revisions (storage/ddl/candle_revisions.sql),
    which backfill/ raises whenever it changes candles_1m's history: 0 if
    nothing has, and 0 if the table doesn't exist yet (a database set up
    before Phase 3a, until init_db.py runs again). indicator_state.py
    rebuilds a symbol's indicators when this moves.
    """
    with psycopg.connect(dsn) as conn:
        try:
            row = conn.execute("SELECT revision FROM candle_revisions WHERE symbol = %s", (symbol,)).fetchone()
        except psycopg.errors.UndefinedTable:
            return 0
    return row[0] if row else 0
