"""
ws.py

WebSocket endpoint for live candle updates — the "WebSocket for live push
from Redis pub/sub" row of DSView's architecture table (see CLAUDE.md).

Deliberately serves only the *current*, live-updating candle. Historical
backfill (scrolling a chart back in time) is REST's job: routers/candles.py,
reading Postgres.

Pushes through push.py's push_until_disconnect, which watches for the
client leaving instead of waiting for a send to fail. Without it, a
closed tab kept its Redis subscription until the next candle — forever
while candle_aggregator.py isn't running, hanging uvicorn --reload.

Author: @DS
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, WebSocket, status

from redis_client import CandleRedisReader
from routers.pipelines import PipelineName, pipeline_query, resolve
from routers.push import push_until_disconnect

logger = logging.getLogger(__name__)

router = APIRouter()

# The only timeframe candle_aggregator.py writes (CandleRedisSink's default).
# TODO(@DS): make timeframe a query param once more than "1m" exists.
DEFAULT_TIMEFRAME = "1m"


@router.websocket("/ws/candles/{symbol}")
async def candle_updates(websocket: WebSocket, symbol: str, pipeline: PipelineName = pipeline_query()) -> None:
    """Push the live-updating candle for `symbol` (in `pipeline`) until the client disconnects."""
    await websocket.accept()
    reader = CandleRedisReader(prefix=resolve(pipeline).redis_prefix)
    try:
        # Cancelling the push loop unsubscribes (subscribe_updates' finally).
        error = await push_until_disconnect(websocket, _push_candles(websocket, reader, symbol))
    finally:
        await reader.close()

    if error is None:
        logger.info("Client disconnected from candle stream for %s", symbol)
        return

    # In practice Redis being unreachable; 1011 tells the client so.
    logger.error("Candle stream for %s failed: %r", symbol, error)
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="candle updates unavailable")


async def _push_candles(websocket: WebSocket, reader: CandleRedisReader, symbol: str) -> None:
    """Send the cached current candle, if any, then every update as it's published."""
    latest = await reader.get_latest(symbol, DEFAULT_TIMEFRAME)
    if latest is not None:
        await websocket.send_text(latest)

    async for update in reader.subscribe_updates(symbol, DEFAULT_TIMEFRAME):
        await websocket.send_text(update)
