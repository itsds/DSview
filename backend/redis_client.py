"""
redis_client.py

Async Redis boundary for the backend service — the read-side counterpart
to streaming/redis_sink.py's CandleRedisSink, FootprintRedisSink and
IndicatorRedisSink. Kept as its own module so routers/ws.py,
routers/indicators.py and routers/footprint.py never touch `redis` directly, mirroring the same producer/consumer-boundary pattern
used elsewhere in this repo (see ingestion/producer.py).

Author: @DS
"""

from __future__ import annotations

from typing import AsyncIterator, Optional

import redis.asyncio as redis

from config.settings import REDIS_HOST, REDIS_PORT


class CandleRedisReader:
    """Reads the current cached candle or indicators, and their live pub/sub updates, from Redis."""

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)

    async def get_latest(self, symbol: str, timeframe: str) -> Optional[str]:
        """
        Fetch the current cached candle JSON, if any.

        Lets a client that connects mid-window see current state right
        away instead of waiting for the next candle_aggregator.py batch
        to publish — see redis_sink.py's docstring.
        """
        return await self._client.get(f"candle:{symbol}:{timeframe}")

    def subscribe_updates(self, symbol: str, timeframe: str) -> AsyncIterator[str]:
        """Yield each candle update JSON as CandleRedisSink publishes it."""
        return self._subscribe(f"candle_updates:{symbol}:{timeframe}")

    async def get_latest_indicators(self, symbol: str, timeframe: str) -> Optional[str]:
        """The newest bar's cached indicators JSON for `symbol` at `timeframe`, if any (IndicatorRedisSink)."""
        return await self._client.get(f"indicators:{symbol}:{timeframe}")

    def subscribe_indicator_updates(self, symbol: str, timeframe: str) -> AsyncIterator[str]:
        """Yield each indicators JSON as IndicatorRedisSink publishes it."""
        return self._subscribe(f"indicator_updates:{symbol}:{timeframe}")

    async def get_latest_footprint(self, symbol: str) -> Optional[str]:
        """The newest minute's cached footprint grid JSON for `symbol`, if any (FootprintRedisSink)."""
        return await self._client.get(f"footprint:{symbol}:1m")

    def subscribe_footprint_updates(self, symbol: str) -> AsyncIterator[str]:
        """Yield each minute's footprint grid JSON as FootprintRedisSink publishes it."""
        return self._subscribe(f"footprint_updates:{symbol}:1m")

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
