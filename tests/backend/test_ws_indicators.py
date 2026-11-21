"""Tests for /ws/indicators/{symbol} (backend/routers/indicators.py + redis_client.py) over real Redis."""

from __future__ import annotations

import json
import threading
from functools import partial

import pytest
from starlette.websockets import WebSocketDisconnect

from redis_client import CandleRedisReader

from helpers import wait_for


def _subscribers(redis_client, channel: str) -> int:
    return dict(redis_client.pubsub_numsub(channel))[channel]


@pytest.fixture
def reader_closed(monkeypatch) -> threading.Event:
    """Set once the route has closed its CandleRedisReader: the handler's last step."""
    closed = threading.Event()

    class TrackedReader(CandleRedisReader):
        async def close(self) -> None:
            await super().close()
            closed.set()

    monkeypatch.setattr("routers.indicators.CandleRedisReader", TrackedReader)
    return closed


def _message(symbol, timeframe, start, ema_20):
    return json.dumps(
        {
            "symbol": symbol,
            "timeframe": timeframe,
            "window_start": start,
            "indicators": {"ema_20": ema_20, "ema_50": None, "ema_200": None},
        }
    )


def test_sends_the_cached_bar_then_every_published_one_for_that_timeframe(
    client, redis_client, symbol, reader_closed
):
    channel = f"indicator_updates:{symbol}:5m"
    cached = _message(symbol, "5m", "2026-11-20T10:15:00+00:00", 97000.5)
    redis_client.set(f"indicators:{symbol}:5m", cached, ex=60)
    redis_client.set(f"indicators:{symbol}:1m", _message(symbol, "1m", "2026-11-20T10:19:00+00:00", 1.0), ex=60)
    try:
        with client.websocket_connect(f"/ws/indicators/{symbol}?timeframe=5m") as ws:
            assert ws.receive_text() == cached

            wait_for(lambda: _subscribers(redis_client, channel) == 1, timeout=5)
            redis_client.publish(f"indicator_updates:{symbol}:1m", _message(symbol, "1m", "x", 2.0))  # another timeframe
            revised = _message(symbol, "5m", "2026-11-20T10:10:00+00:00", 96990.0)  # a late trade's revision
            update = _message(symbol, "5m", "2026-11-20T10:15:00+00:00", 97001.0)
            redis_client.publish(channel, revised)
            redis_client.publish(channel, update)
            assert ws.receive_text() == revised
            assert ws.receive_text() == update

            ws.close()
            wait_for(lambda: _subscribers(redis_client, channel) == 0, timeout=5, message="subscription outlived the socket")
            assert reader_closed.wait(timeout=5), "the handler never closed its Redis client"
    finally:
        redis_client.delete(f"indicators:{symbol}:5m", f"indicators:{symbol}:1m")


def test_timeframe_defaults_to_1m(client, redis_client, symbol, reader_closed):
    with client.websocket_connect(f"/ws/indicators/{symbol}") as ws:
        wait_for(lambda: _subscribers(redis_client, f"indicator_updates:{symbol}:1m") == 1, timeout=5)
        ws.close()
        assert reader_closed.wait(timeout=5)


def test_unknown_timeframe_is_closed_with_1008(client, symbol):
    with client.websocket_connect(f"/ws/indicators/{symbol}?timeframe=2m") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1008


def test_closes_with_1011_when_redis_is_unreachable(client, monkeypatch, closed_port, symbol):
    monkeypatch.setattr("routers.indicators.CandleRedisReader", partial(CandleRedisReader, port=closed_port))

    with client.websocket_connect(f"/ws/indicators/{symbol}?timeframe=5m") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1011
