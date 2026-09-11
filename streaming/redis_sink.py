"""
redis_sink.py

Redis serving-layer writer for windowed candle output.

Structured Streaming has no built-in Redis sink, so candle_aggregator.py
drives this through foreachBatch (see its module docstring for why).
Kept as its own module — rather than inline in candle_aggregator.py — so
the write logic lives in one place if indicators.py/footprint.py later
need to push their own state through the same Redis instance.

Only the *latest* candle per symbol/timeframe is kept (one key,
overwritten every batch) — Redis here is real-time serving state, not
history. Historical candles are Postgres's job once that layer exists.

Writes go two places, per the "WebSocket for live push from Redis
pub/sub" row of CLAUDE.md's architecture table:
  - a SET with TTL (`candle:{symbol}:{timeframe}`) so a client connecting
    mid-window can fetch current state immediately instead of waiting for
    the next update;
  - a PUBLISH (`candle_updates:{symbol}:{timeframe}`) so already-connected
    clients (backend/routers/ws.py) get pushed each update as it happens.

Author: @DS
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

import redis

logger = logging.getLogger(__name__)

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
REDIS_HOST = "localhost"
REDIS_PORT = 6379
# A stale key (aggregator crashed/stopped) should read as "no live data"
# downstream rather than silently serve an arbitrarily old candle forever.
CANDLE_KEY_TTL_SECONDS = 120


def _json_default(value: Any) -> str:
    """Same Decimal/datetime handling as ingestion/producer.py's _json_default."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class CandleRedisSink:
    """Writes the latest candle per symbol/timeframe to Redis as a JSON string."""

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        timeframe: str = "1m",
        ttl_seconds: int = CANDLE_KEY_TTL_SECONDS,
    ) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._timeframe = timeframe
        self._ttl_seconds = ttl_seconds

    def write_candle(self, candle: Mapping[str, Any]) -> None:
        """Cache this candle's current fields and publish the update for live subscribers."""
        symbol = candle["symbol"]
        value = json.dumps(dict(candle), default=_json_default)

        self._client.set(f"candle:{symbol}:{self._timeframe}", value, ex=self._ttl_seconds)
        self._client.publish(f"candle_updates:{symbol}:{self._timeframe}", value)
