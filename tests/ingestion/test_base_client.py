"""Tests for ingestion/base_client.py's connection lifecycle, against a local WebSocket server."""

from __future__ import annotations

import asyncio
from typing import List, Optional

import websockets

from base_client import BaseExchangeClient

from helpers import async_wait_for


class EchoClient(BaseExchangeClient):
    """Treats each text frame as one event; a frame reading "bad" fails to parse."""

    def __init__(self, url: str, on_event) -> None:
        super().__init__(symbols=["TESTUSDT"], on_event=on_event)
        self._url = url

    @property
    def ws_url(self) -> str:
        return self._url

    def build_subscription_message(self) -> Optional[str]:
        return "subscribe"

    async def parse_message(self, raw_message: str) -> List[str]:
        if raw_message == "bad":
            raise ValueError("unparseable frame")
        return [raw_message]


async def test_subscribes_skips_bad_frames_and_reconnects_after_a_drop():
    connections = 0
    subscriptions: List[str] = []

    async def handler(ws) -> None:
        nonlocal connections
        connections += 1
        subscriptions.append(await ws.recv())
        await ws.send("bad")
        await ws.send(f"frame-{connections}")
        # Then hang up, the way an exchange drops a connection.

    events: List[str] = []

    async def on_event(event: str) -> None:
        events.append(event)

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = EchoClient(f"ws://127.0.0.1:{port}", on_event)
        task = asyncio.create_task(client.start())
        try:
            # The second frame only arrives over a second connection.
            await async_wait_for(lambda: len(events) >= 2, timeout=10)
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5)

    assert events[:2] == ["frame-1", "frame-2"]  # "bad" was skipped, not fatal
    assert subscriptions[:2] == ["subscribe", "subscribe"]  # resubscribed on reconnect


async def test_stop_ends_start_while_connected():
    async def handler(ws) -> None:
        await ws.recv()
        await ws.wait_closed()  # stay connected and silent until the client leaves

    async def on_event(event) -> None:
        pass

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = EchoClient(f"ws://127.0.0.1:{port}", on_event)
        task = asyncio.create_task(client.start())
        await async_wait_for(lambda: client._ws is not None, timeout=5)
        await client.stop()
        await asyncio.wait_for(task, timeout=5)  # returns, rather than reconnecting
