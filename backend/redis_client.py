"""
redis_client.py

Async Redis boundary for the backend service — the read-side counterpart
to streaming/redis_sink.py's write-side CandleRedisSink. Kept as its own
module so routers/ws.py never touches `redis` directly, mirroring the
same producer/consumer-boundary pattern used elsewhere in this repo (see
ingestion/producer.py).

Author: @DS
"""

from __future__ import annotations

from typing import AsyncIterator, Optional

import redis.asyncio as redis

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
REDIS_HOST = "localhost"
REDIS_PORT = 6379


class CandleRedisReader:
    """Reads the current cached candle and live pub/sub updates from Redis."""

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

    async def subscribe_updates(self, symbol: str, timeframe: str) -> AsyncIterator[str]:
        """Yield each candle update JSON as CandleRedisSink publishes it."""
        pubsub = self._client.pubsub()
        channel = f"candle_updates:{symbol}:{timeframe}"
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
