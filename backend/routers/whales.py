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

Pushes through push.py's push_until_disconnect, which watches for the
client leaving instead of waiting for a send to fail: whales are rare, so
a closed tab could otherwise keep its Kafka consumer open until the next.

Author: @DS
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Query, WebSocket, status

from kafka_client import WhaleAlertReader
from routers.pipelines import PipelineName, pipeline_query, resolve
from routers.push import push_until_disconnect

logger = logging.getLogger(__name__)

router = APIRouter()

DEFAULT_BACKLOG = 50
MAX_BACKLOG = 500


@router.websocket("/ws/whales")
async def whale_alerts(
    websocket: WebSocket,
    backlog: int = Query(DEFAULT_BACKLOG, ge=0, le=MAX_BACKLOG),
    pipeline: PipelineName = pipeline_query(),
) -> None:
    """Push the latest `backlog` whale alerts, then live ones, until the client disconnects."""
    await websocket.accept()
    # Cancelling the push loop closes its Kafka consumer (the reader's finally).
    error = await push_until_disconnect(websocket, _forward_alerts(websocket, backlog, resolve(pipeline).whale_alerts_topic))
    if error is None:
        logger.info("Client disconnected from whale alerts")
        return

    # The alert stream never ends on its own, so forwarding stopped on an
    # error: in practice the broker being unreachable while the stream was
    # starting (see WhaleAlertReader.stream). 1011 tells the client to
    # reconnect; the backlog replays whatever it missed.
    logger.error("Whale alert stream failed: %r", error)
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="whale alerts unavailable")


async def _forward_alerts(websocket: WebSocket, backlog: int, topic: str) -> None:
    async for alert in WhaleAlertReader(topic=topic).stream(backlog):
        await websocket.send_text(alert)

