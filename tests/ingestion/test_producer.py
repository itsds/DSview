"""Tests for ingestion/producer.py against a real broker."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from config.settings import DEPTH_TOPIC, TRADES_TOPIC

from producer import MarketDataProducer
from schema import DepthLevel, DepthUpdate, Exchange

from helpers import TopicTail, make_trade


async def test_trade_lands_on_the_trades_topic_keyed_by_symbol(trade_producer, symbol):
    trade = make_trade(symbol, "97000.10", "2.5", side="sell")

    async with TopicTail(TRADES_TOPIC) as tail:
        await trade_producer.send(trade)
        message = await tail.next_message(lambda m: m.key == symbol.encode())

    payload = json.loads(message.value)
    assert payload == json.loads(json.dumps(trade.model_dump(mode="json")))
    assert payload["price"] == "97000.10"
    assert payload["side"] == "sell"


async def test_depth_update_lands_on_the_depth_topic(trade_producer, symbol):
    depth = DepthUpdate(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        event_time=datetime.now(timezone.utc),
        bids=[DepthLevel(price=Decimal("100.5"), quantity=Decimal("3"))],
        asks=[DepthLevel(price=Decimal("100.6"), quantity=Decimal("1"))],
    )

    async with TopicTail(DEPTH_TOPIC) as tail:
        await trade_producer.send(depth)
        message = await tail.next_message(lambda m: m.key == symbol.encode())

    payload = json.loads(message.value)
    assert payload["event_type"] == "depth"
    assert payload["bids"] == [{"price": "100.5", "quantity": "3"}]


async def test_send_before_start_is_an_error(symbol):
    with pytest.raises(RuntimeError):
        await MarketDataProducer().send(make_trade(symbol, "1", "1"))
