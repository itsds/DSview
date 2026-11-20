"""
whales.py

WebSocket endpoint for whale alerts, the backend end of the alerts lane:
streaming/jobs/whale_detector.py -> alerts.whales -> here -> the right
panel's whale list. Each message is one alert's JSON exactly as the
detector published it (the shape is in whale_detector.py's docstring).

On connect, a client first gets the most recent `backlog` alerts still on
the topic, oldest first, so a freshly opened page isn't empty until the
next whale trades; then every new alert as it's published. Alerts can
repeat (delivery is best-effort, see whale_detector.py), so a client
should dedupe on (exchange, trade_id).

Unlike routers/ws.py, this watches for the client leaving rather than
waiting for a send to fail: whales are rare, so a closed tab could
otherwise keep its Kafka consumer open until the next one.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect, status

from kafka_client import WhaleAlertReader

logger = logging.getLogger(__name__)

router = APIRouter()

DEFAULT_BACKLOG = 50
MAX_BACKLOG = 500


@router.websocket("/ws/whales")
async def whale_alerts(
    websocket: WebSocket,
    backlog: int = Query(DEFAULT_BACKLOG, ge=0, le=MAX_BACKLOG),
) -> None:
    """Push the latest `backlog` whale alerts, then live ones, until the client disconnects."""
    await websocket.accept()
    forward = asyncio.create_task(_forward_alerts(websocket, backlog))
    client_gone = asyncio.create_task(_wait_for_disconnect(websocket))
    try:
        done, _ = await asyncio.wait({forward, client_gone}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Cancelling `forward` closes its Kafka consumer (the reader's finally).
        for task in (forward, client_gone):
            task.cancel()
        await asyncio.gather(forward, client_gone, return_exceptions=True)

    error = None if forward.cancelled() else forward.exception()
    if client_gone in done or isinstance(error, WebSocketDisconnect):
        logger.info("Client disconnected from whale alerts")
        return

    # The alert stream never ends on its own, so forwarding stopped on an
    # error: in practice the broker being unreachable while the stream was
    # starting (see WhaleAlertReader.stream). 1011 tells the client to
    # reconnect; the backlog replays whatever it missed.
    logger.error("Whale alert stream failed: %r", error or "ended without an error")
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="whale alerts unavailable")


async def _forward_alerts(websocket: WebSocket, backlog: int) -> None:
    async for alert in WhaleAlertReader().stream(backlog):
        await websocket.send_text(alert)


async def _wait_for_disconnect(websocket: WebSocket) -> None:
    """Return once the client goes away. Anything it sends is ignored."""
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
