"""Tests for streaming/alert_publisher.py against a real broker."""

from __future__ import annotations

import json

import pytest

from config.settings import WHALE_ALERTS_TOPIC

from alert_publisher import AlertPublisher

from helpers import TopicTail


async def test_publishes_json_keyed_by_symbol(kafka_topics, symbol):
    publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC)
    await publisher.start()
    try:
        async with TopicTail(WHALE_ALERTS_TOPIC) as tail:
            await publisher.publish({"symbol": symbol, "trade_id": "1", "notional": "123456.7"})
            message = await tail.next_message(lambda m: m.key == symbol.encode())
    finally:
        await publisher.stop()

    assert json.loads(message.value) == {"symbol": symbol, "trade_id": "1", "notional": "123456.7"}


async def test_publish_before_start_is_an_error():
    with pytest.raises(RuntimeError):
        await AlertPublisher(topic=WHALE_ALERTS_TOPIC).publish({"symbol": "X"})
