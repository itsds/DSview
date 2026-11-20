"""Tests for /ws/whales (backend/routers/whales.py + kafka_client.py) over a real broker."""

from __future__ import annotations

import asyncio
import json
from functools import partial

import pytest
from starlette.websockets import WebSocketDisconnect

from config.settings import WHALE_ALERTS_TOPIC

from alert_publisher import AlertPublisher
from kafka_client import WhaleAlertReader


def _publish(*alerts) -> None:
    async def publish() -> None:
        publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC)
        await publisher.start()
        try:
            for alert in alerts:
                await publisher.publish(alert)
        finally:
            await publisher.stop()

    asyncio.run(publish())


def _alert(symbol: str, trade_id: str) -> dict:
    return {"event_type": "whale_trade", "exchange": "binance", "symbol": symbol, "trade_id": trade_id}


def test_replays_the_backlog_oldest_first_then_pushes_live_alerts(client, kafka_topics, symbol):
    _publish(*(_alert(symbol, str(i)) for i in range(3)))

    with client.websocket_connect("/ws/whales?backlog=3") as ws:
        backlog = [json.loads(ws.receive_text()) for _ in range(3)]
        assert backlog == [_alert(symbol, str(i)) for i in range(3)]

        _publish(_alert(symbol, "live"))
        assert json.loads(ws.receive_text()) == _alert(symbol, "live")


def test_closes_with_1011_when_kafka_is_unreachable(client, monkeypatch, closed_port):
    monkeypatch.setattr(
        "routers.whales.WhaleAlertReader",
        partial(WhaleAlertReader, bootstrap_servers=f"127.0.0.1:{closed_port}"),
    )

    with client.websocket_connect("/ws/whales") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1011


def test_backlog_is_capped(client, test_services):
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/whales?backlog=501") as ws:
            ws.receive_text()
