"""Tests for /ws/candles/{symbol} (backend/routers/ws.py + redis_client.py) over real Redis."""

from __future__ import annotations

import json
import threading
from functools import partial

import pytest
from starlette.websockets import WebSocketDisconnect

from redis_client import CandleRedisReader

from helpers import wait_for


def _subscribers(redis_client, symbol: str) -> int:
    return dict(redis_client.pubsub_numsub(f"candle_updates:{symbol}:1m"))[f"candle_updates:{symbol}:1m"]


@pytest.fixture
def reader_closed(monkeypatch) -> threading.Event:
    """Set once the route has closed its CandleRedisReader: the handler's last step."""
    closed = threading.Event()  # the app runs in TestClient's own thread

    class TrackedReader(CandleRedisReader):
        async def close(self) -> None:
            await super().close()
            closed.set()

    monkeypatch.setattr("routers.ws.CandleRedisReader", TrackedReader)
    return closed


def _leave(ws, redis_client, symbol: str, reader_closed: threading.Event) -> None:
    """
    Close the socket the way a browser tab does, and wait for the handler to let go.

    Leaving the `with` block alone isn't enough: TestClient then cancels
    the app straight away, cutting its cleanup short, where uvicorn lets a
    handler finish after the client goes.
    """
    ws.close()
    wait_for(lambda: _subscribers(redis_client, symbol) == 0, timeout=5, message="subscription outlived the socket")
    assert reader_closed.wait(timeout=5), "the handler never closed its Redis client"


def test_sends_the_cached_candle_then_every_published_update(client, redis_client, symbol, reader_closed):
    cached = json.dumps({"symbol": symbol, "window_start": "2026-11-20T10:00:00+00:00", "close": "100"})
    redis_client.set(f"candle:{symbol}:1m", cached, ex=60)
    try:
        with client.websocket_connect(f"/ws/candles/{symbol}") as ws:
            assert ws.receive_text() == cached

            wait_for(lambda: _subscribers(redis_client, symbol) == 1, timeout=5)
            update = json.dumps({"symbol": symbol, "window_start": "2026-11-20T10:00:00+00:00", "close": "101"})
            redis_client.publish(f"candle_updates:{symbol}:1m", update)
            assert ws.receive_text() == update
            _leave(ws, redis_client, symbol, reader_closed)
    finally:
        redis_client.delete(f"candle:{symbol}:1m")


def test_a_closed_tab_releases_its_subscription_at_once(client, redis_client, symbol, reader_closed):
    # Regression: the handler used to notice a closed socket only at its next
    # send, so a quiet channel kept the subscription (and uvicorn --reload) forever.
    with client.websocket_connect(f"/ws/candles/{symbol}") as ws:
        wait_for(lambda: _subscribers(redis_client, symbol) == 1, timeout=5)
        _leave(ws, redis_client, symbol, reader_closed)  # nothing is ever published, so no send can fail


def test_closes_with_1011_when_redis_is_unreachable(client, monkeypatch, closed_port, symbol):
    monkeypatch.setattr("routers.ws.CandleRedisReader", partial(CandleRedisReader, port=closed_port))

    with client.websocket_connect(f"/ws/candles/{symbol}") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1011
