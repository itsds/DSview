"""
ws.py

WebSocket endpoint for live candle updates — the "WebSocket for live push
from Redis pub/sub" row of DSView's architecture table (see CLAUDE.md).

Deliberately serves only the *current*, live-updating candle. Historical
backfill (scrolling a chart back in time) is REST's job and needs the
Postgres/storage layer, which doesn't exist yet — see this router's
future sibling, routers/candles.py.

Author: @DS
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from redis_client import CandleRedisReader

logger = logging.getLogger(__name__)

router = APIRouter()

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded;
# also make timeframe a query param once more than "1m" exists.
DEFAULT_TIMEFRAME = "1m"


@router.websocket("/ws/candles/{symbol}")
async def candle_updates(websocket: WebSocket, symbol: str) -> None:
    """Push the live-updating candle for `symbol` until the client disconnects."""
    await websocket.accept()
    reader = CandleRedisReader()

    try:
        latest = await reader.get_latest(symbol, DEFAULT_TIMEFRAME)
        if latest is not None:
            await websocket.send_text(latest)

        async for update in reader.subscribe_updates(symbol, DEFAULT_TIMEFRAME):
            await websocket.send_text(update)
    except WebSocketDisconnect:
        logger.info("Client disconnected from candle stream for %s", symbol)
    finally:
        await reader.close()
