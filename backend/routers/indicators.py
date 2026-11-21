"""
indicators.py

WebSocket endpoint for live indicator values — the live half of what
streaming/jobs/indicators.py computes in Spark; the history half rides on
GET /candles (routers/candles.py), one `indicators` object per candle.

/ws/indicators/{symbol}?timeframe=5m sends the newest bar's cached values
on connect, then every bar Spark changes, each as IndicatorRedisSink
writes it:

    {"symbol": "BTCUSDT", "timeframe": "5m", "window_start": "2026-11-20T10:15:00+00:00",
     "indicators": {"ema_20": 97012.4, "ema_50": 96980.1, "ema_200": null}}

Mostly the bar in progress, but a late trade can revise the bar before it,
so a client must place each message by its window_start rather than
assume the newest bar. Unlike /ws/candles, which streams 1m only and
leaves the roll-up to the client, this is per timeframe: an EMA on 5m
bars isn't derivable from the 1m one.

Like ws.py: pushes through push.py's push_until_disconnect, and closes
with 1011 if Redis is unreachable. An unknown timeframe is closed with
1008 (policy violation), there being no 422 for a WebSocket.

Author: @DS
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, WebSocket, status

from config.settings import TIMEFRAMES

from redis_client import CandleRedisReader
from routers.push import push_until_disconnect

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/indicators/{symbol}")
async def indicator_updates(websocket: WebSocket, symbol: str, timeframe: str = "1m") -> None:
    """Push `symbol`'s indicator values at `timeframe` until the client disconnects."""
    await websocket.accept()
    if timeframe not in TIMEFRAMES:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"timeframe must be one of {list(TIMEFRAMES)}")
        return

    reader = CandleRedisReader()
    try:
        error = await push_until_disconnect(websocket, _push_indicators(websocket, reader, symbol, timeframe))
    finally:
        await reader.close()

    if error is None:
        logger.info("Client disconnected from indicator stream for %s %s", symbol, timeframe)
        return

    logger.error("Indicator stream for %s %s failed: %r", symbol, timeframe, error)
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="indicator updates unavailable")


async def _push_indicators(websocket: WebSocket, reader: CandleRedisReader, symbol: str, timeframe: str) -> None:
    """Send the cached newest bar, if any, then every update as it's published."""
    latest = await reader.get_latest_indicators(symbol, timeframe)
    if latest is not None:
        await websocket.send_text(latest)

    async for update in reader.subscribe_indicator_updates(symbol, timeframe):
        await websocket.send_text(update)
