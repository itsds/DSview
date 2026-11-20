"""
push.py

The one way this backend's WebSocket routes push to a client: run the
push loop and watch for the client leaving, side by side.

A server that only ever sends notices a closed connection when its next
send fails. For a route whose source can go quiet, that's a problem:
/ws/whales may wait hours for the next whale, and /ws/candles waits
forever whenever candle_aggregator.py isn't running. Meanwhile the
handler keeps its Kafka consumer or Redis subscription open, and
uvicorn --reload hangs at "Waiting for background tasks to complete",
because it waits for every handler to finish. Watching with a
concurrent receive() ends the handler as soon as the client goes.

Author: @DS
"""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Optional

from fastapi import WebSocket, WebSocketDisconnect


async def push_until_disconnect(websocket: WebSocket, push: Coroutine[Any, Any, None]) -> Optional[BaseException]:
    """
    Run `push` (a loop sending to `websocket`) until the client leaves or it stops.

    Returns None if the client disconnected, else what stopped the push
    loop: its exception, or a RuntimeError if it just returned — every
    push loop here is meant to run until the client leaves. Either way
    `push` has been cancelled and has finished by the time this returns,
    so its finally blocks (closing a consumer, unsubscribing) have run.
    """
    pushing = asyncio.create_task(push)
    client_gone = asyncio.create_task(_wait_for_disconnect(websocket))
    try:
        done, _ = await asyncio.wait({pushing, client_gone}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (pushing, client_gone):
            task.cancel()
        await asyncio.gather(pushing, client_gone, return_exceptions=True)

    error = None if pushing.cancelled() else pushing.exception()
    if client_gone in done or isinstance(error, WebSocketDisconnect):
        return None
    return error or RuntimeError("push loop ended without an error")


async def _wait_for_disconnect(websocket: WebSocket) -> None:
    """Return once the client goes away. Anything it sends is ignored."""
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
