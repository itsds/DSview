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


async def test_send_batch_delivers_every_event_in_order(trade_producer, symbol):
    trades = [make_trade(symbol, f"97000.{i:02d}", "0.1", trade_id=str(i)) for i in range(50)]

    async with TopicTail(TRADES_TOPIC) as tail:
        await trade_producer.send_batch(trades)
        received = [await tail.next_json(lambda m: m["symbol"] == symbol) for _ in trades]

    assert [m["trade_id"] for m in received] == [str(i) for i in range(50)]


async def test_a_producer_for_another_pipeline_sends_to_its_topics(kafka_topics, symbol):
    from config.settings import REPLAY_PIPELINE

    producer = MarketDataProducer(pipeline=REPLAY_PIPELINE)
    await producer.start()
    try:
        async with TopicTail(REPLAY_PIPELINE.trades_topic) as replay_tail, TopicTail(TRADES_TOPIC) as live_tail:
            await producer.send_batch([make_trade(symbol, "97000", "1", trade_id="r1")])
            assert (await replay_tail.next_json(lambda m: m["symbol"] == symbol))["trade_id"] == "r1"
            assert await live_tail.collect_json(lambda m: m["symbol"] == symbol, quiet=1.0) == []
    finally:
        await producer.stop()
