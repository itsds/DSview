"""
redis_client.py

Async Redis boundary for the backend service — the read-side counterpart
to streaming/redis_sink.py's CandleRedisSink, FootprintRedisSink and
IndicatorRedisSink, plus the replay service's controls
(ReplayControlClient). Kept as its own module so routers/ws.py,
routers/indicators.py, routers/footprint.py and routers/replay.py never touch `redis` directly, mirroring the same producer/consumer-boundary pattern
used elsewhere in this repo (see ingestion/producer.py).

Author: @DS
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Optional

import redis.asyncio as redis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PREFIX, REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY


class CandleRedisReader:
    """Reads the current cached candle or indicators, and their live pub/sub updates, from Redis."""

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT, prefix: str = REDIS_PREFIX) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        # The pipeline's (config/settings.py's Pipeline.redis_prefix): "" for live, "replay:" for replay.
        self._prefix = prefix

    async def get_latest(self, symbol: str, timeframe: str) -> Optional[str]:
        """
        Fetch the current cached candle JSON, if any.

        Lets a client that connects mid-window see current state right
        away instead of waiting for the next candle_aggregator.py batch
        to publish — see redis_sink.py's docstring.
        """
        return await self._client.get(f"{self._prefix}candle:{symbol}:{timeframe}")

    def subscribe_updates(self, symbol: str, timeframe: str) -> AsyncIterator[str]:
        """Yield each candle update JSON as CandleRedisSink publishes it."""
        return self._subscribe(f"{self._prefix}candle_updates:{symbol}:{timeframe}")

    async def get_latest_indicators(self, symbol: str, timeframe: str) -> Optional[str]:
        """The newest bar's cached indicators JSON for `symbol` at `timeframe`, if any (IndicatorRedisSink)."""
        return await self._client.get(f"{self._prefix}indicators:{symbol}:{timeframe}")

    def subscribe_indicator_updates(self, symbol: str, timeframe: str) -> AsyncIterator[str]:
        """Yield each indicators JSON as IndicatorRedisSink publishes it."""
        return self._subscribe(f"{self._prefix}indicator_updates:{symbol}:{timeframe}")

    async def get_latest_footprint(self, symbol: str) -> Optional[str]:
        """The newest minute's cached footprint grid JSON for `symbol`, if any (FootprintRedisSink)."""
        return await self._client.get(f"{self._prefix}footprint:{symbol}:1m")

    def subscribe_footprint_updates(self, symbol: str) -> AsyncIterator[str]:
        """Yield each minute's footprint grid JSON as FootprintRedisSink publishes it."""
        return self._subscribe(f"{self._prefix}footprint_updates:{symbol}:1m")

    async def _subscribe(self, channel: str) -> AsyncIterator[str]:
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield message["data"]
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    async def close(self) -> None:
        await self._client.aclose()


class ReplayControlClient:
    """
    The backend's side of the replay service's controls (replay/control.py
    is the other): commands go onto a list the service pops, and its status
    is a key it rewrites every second, gone within seconds of it stopping.
    """

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)

    async def status(self) -> Optional[Dict[str, Any]]:
        """The service's latest status, or None if it isn't running."""
        raw = await self._client.get(REPLAY_STATUS_KEY)
        return None if raw is None else json.loads(raw)

    async def send(self, command: Dict[str, Any]) -> None:
        await self._client.lpush(REPLAY_COMMANDS_KEY, json.dumps(command))

    async def close(self) -> None:
        await self._client.aclose()
