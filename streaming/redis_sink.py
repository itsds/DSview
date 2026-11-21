"""
redis_sink.py

Redis serving-layer writers for windowed output: CandleRedisSink for
candle_aggregator.py's candles, IndicatorRedisSink for jobs/indicators.py's
indicator values.

Structured Streaming has no built-in Redis sink, so candle_aggregator.py
drives this through foreachBatch (see its module docstring for why).
Kept as its own module — rather than inline in candle_aggregator.py — so
the write logic lives in one place for every job that pushes state
through the same Redis instance (indicators.py now, footprint.py later).

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
from collections import defaultdict
from typing import Any, Iterable, List, Mapping

import redis

from config.settings import REDIS_HOST, REDIS_PORT

logger = logging.getLogger(__name__)

# A stale key (aggregator crashed/stopped) should read as "no live data"
# downstream rather than silently serve an arbitrarily old candle forever.
CANDLE_KEY_TTL_SECONDS = 120
# The same reasoning, for a stopped indicator job.
INDICATOR_KEY_TTL_SECONDS = 120
# Bars per symbol and timeframe a batch publishes at most: its newest ones,
# which are all a live batch ever changes (the bar in progress, plus up to
# two not yet settled; see indicator_engine.py). A bigger batch is a warm-up
# or catch-up rewriting history, which charts get over REST, not pushed one
# bar at a time.
INDICATOR_PUBLISH_BARS = 3


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


class IndicatorRedisSink:
    """
    Writes indicator values for live serving, the way CandleRedisSink writes
    candles: per symbol and timeframe, a TTL'd SET of the newest bar
    (`indicators:{symbol}:{timeframe}`) for clients that connect mid-bar, and
    a PUBLISH of each changed bar (`indicator_updates:{symbol}:{timeframe}`)
    for clients already connected (backend/routers/indicators.py).

    Each message: {"symbol", "timeframe", "window_start" (ISO 8601, UTC),
    "indicators": {"ema_20": value or null, ...}} — the `indicators` object
    has the same shape GET /candles gives each candle.
    """

    def __init__(
        self, host: str = REDIS_HOST, port: int = REDIS_PORT, ttl_seconds: int = INDICATOR_KEY_TTL_SECONDS
    ) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._ttl_seconds = ttl_seconds

    def write_rows(self, rows: Iterable[Mapping[str, Any]], columns: List[str]) -> None:
        """`rows` (symbol, timeframe, window_start and `columns`) oldest first within each symbol and timeframe."""
        by_series = defaultdict(list)
        for row in rows:
            by_series[(row["symbol"], row["timeframe"])].append(row)

        pipe = self._client.pipeline(transaction=False)
        for (symbol, timeframe), series in by_series.items():
            for row in series[-INDICATOR_PUBLISH_BARS:]:
                value = json.dumps(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "window_start": row["window_start"],
                        "indicators": {column: row[column] for column in columns},
                    },
                    default=_json_default,
                )
                pipe.publish(f"indicator_updates:{symbol}:{timeframe}", value)
            pipe.set(f"indicators:{symbol}:{timeframe}", value, ex=self._ttl_seconds)
        pipe.execute()
