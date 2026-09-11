"""
postgres_sink.py

Postgres serving-layer writer for the Gold-tier OHLCV candles produced by
candle_aggregator.py.

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

from typing import Any, Mapping

import psycopg

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
POSTGRES_DSN = "postgresql://dsview:dsview@localhost:5432/dsview"

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
