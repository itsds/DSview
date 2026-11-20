"""Tests for streaming/jobs/whale_detector.py: the threshold check, and the detector on a real broker."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from decimal import Decimal

from config.settings import WHALE_ALERTS_TOPIC

from alert_publisher import AlertPublisher
from jobs.whale_detector import WhaleDetector, stop_within

from helpers import TopicTail, make_trade, wait_until_positioned

THRESHOLD = Decimal("100000")


async def _ignore(alert) -> None:
    pass


def _detector(on_whale=_ignore, **kwargs) -> WhaleDetector:
    return WhaleDetector(on_whale=on_whale, threshold_usd=THRESHOLD, **kwargs)


def _payload(price: str, quantity: str, /, **overrides) -> bytes:
    trade = make_trade("BTCUSDT", price, quantity, trade_id="77").model_dump(mode="json")
    trade.update(overrides)  # can make it malformed, which TradeEvent itself wouldn't allow
    return json.dumps(trade).encode()


# --- the threshold check, no broker ------------------------------------------


def test_trade_at_the_threshold_is_a_whale():
    alert = _detector()._handle_message(_payload("50000", "2"))

    assert alert is not None
    assert alert["event_type"] == "whale_trade"
    assert alert["symbol"] == "BTCUSDT"
    assert alert["trade_id"] == "77"
    assert alert["notional"] == "100000"
    assert alert["threshold"] == "100000"


def test_trade_below_the_threshold_is_not():
    assert _detector()._handle_message(_payload("49999.99", "2")) is None


def test_alert_keeps_the_trade_strings_and_writes_plain_decimals():
    alert = _detector()._handle_message(_payload("97000.10000000", "2.50000000", side="sell"))

    assert alert["price"] == "97000.10000000"  # exactly as the trade carried it
    assert alert["quantity"] == "2.50000000"
    assert alert["notional"] == "242500.25"  # no exponent, no trailing zeros
    assert alert["side"] == "sell"
    assert alert["exchange"] == "binance"
    assert alert["event_time"].startswith("20")
    assert alert["detected_at"].endswith("+00:00")


def test_non_trades_and_bad_payloads_are_skipped():
    detector = _detector()
    assert detector._handle_message(json.dumps({"event_type": "depth", "symbol": "BTCUSDT"}).encode()) is None
    assert detector._handle_message(b"not json") is None
    assert detector._handle_message(_payload("1", "1", price="abc")) is None
    assert detector._handle_message(json.dumps({"event_type": "trade", "price": "1", "quantity": "1"}).encode()) is None


# --- on a real broker --------------------------------------------------------


async def _run_detector(on_whale):
    detector = _detector(on_whale, group_id=f"test-whale-detector-{uuid.uuid4().hex}")
    await detector.start()
    await wait_until_positioned(detector._consumer)
    return detector, asyncio.create_task(detector.run())


async def _stop_detector(detector, task) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await detector.stop()


async def test_whale_trades_are_published_to_the_alerts_topic(trade_producer, symbol):
    publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC)
    await publisher.start()
    detector, task = await _run_detector(publisher.publish)
    try:
        async with TopicTail(WHALE_ALERTS_TOPIC) as tail:
            await trade_producer.send(make_trade(symbol, "97000", "0.5", trade_id="small"))
            await trade_producer.send(make_trade(symbol, "97000.10", "2.5", trade_id="big"))
            message = await tail.next_message(lambda m: m.key == symbol.encode())
            later = await tail.collect_json(lambda a: a["symbol"] == symbol)
    finally:
        await _stop_detector(detector, task)
        await publisher.stop()

    alert = json.loads(message.value)
    assert alert["trade_id"] == "big"  # the small trade raised nothing
    assert alert["notional"] == "242500.25"
    assert later == []


async def test_one_failed_hand_off_does_not_stop_detection(trade_producer, symbol):
    delivered = []

    async def flaky(alert) -> None:
        if not delivered and alert["trade_id"] == "first":
            delivered.append(None)
            raise ConnectionError("publish failed")
        delivered.append(alert["trade_id"])

    detector, task = await _run_detector(flaky)
    try:
        await trade_producer.send(make_trade(symbol, "100000", "1", trade_id="first"))
        await trade_producer.send(make_trade(symbol, "100000", "1", trade_id="second"))
        for _ in range(200):
            if "second" in delivered:
                break
            await asyncio.sleep(0.05)
    finally:
        await _stop_detector(detector, task)

    assert delivered == [None, "second"]


# --- bounded shutdown --------------------------------------------------------
# With the broker down, aiokafka's consumer.stop() retries its final commit
# forever. Checked by hand too: Ctrl+C with the broker stopped now exits in
# about 6 s (5 s for the consumer, then the publisher) instead of hanging.


async def test_stop_within_gives_up_on_a_stop_that_never_returns():
    async def never_returns() -> None:
        await asyncio.Event().wait()

    started = time.monotonic()
    assert await stop_within(never_returns(), "Stuck client", timeout=0.2) is False
    assert time.monotonic() - started < 1


async def test_stop_within_lets_a_prompt_stop_finish():
    stopped = []

    async def prompt() -> None:
        await asyncio.sleep(0.01)
        stopped.append(True)

    assert await stop_within(prompt(), "Client", timeout=5) is True
    assert stopped == [True]
