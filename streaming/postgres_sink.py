"""
postgres_sink.py

Postgres serving-layer writers: CandlePostgresSink for the Gold-tier
OHLCV candles produced by candle_aggregator.py, IndicatorPostgresSink for
the indicators jobs/indicators.py computes — plus read_minute_closes(),
which that job warms its indicator state up from. The one place
streaming/ touches psycopg.

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
