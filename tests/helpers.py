"""
helpers.py

Building blocks the tests share: synthetic trades, a Kafka topic tail
that starts at the end of a topic, and polling waits.

Synthetic trades are ingestion/schema.py's own TradeEvent, sent through
ingestion/producer.py's real MarketDataProducer (the `trade_producer`
fixture), so every downstream test reads exactly what live ingestion
would publish.

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import random
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from aiokafka import AIOKafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS

from schema import Exchange, Side, TradeEvent

EMA_REFERENCE_SCRIPT = Path(__file__).resolve().parent / "js" / "ema_reference.mjs"


def make_trade(
    symbol: str,
    price: str,
    quantity: str,
    side: str = "buy",
    event_time: Optional[datetime] = None,
    trade_id: Optional[str] = None,
) -> TradeEvent:
    """A Binance trade as ingestion would normalize it."""
    return TradeEvent(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        event_time=event_time or datetime.now(timezone.utc),
        trade_id=trade_id or uuid.uuid4().hex,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=Side(side),
    )


async def wait_until_positioned(consumer: AIOKafkaConsumer, timeout: float = 30.0) -> None:
    """
    Wait until `consumer` has its partitions and a resolved position on each.

    A consumer with auto_offset_reset="latest" only looks up where "latest"
    is on its first fetch; anything produced before that is skipped. A test
    that produces right after starting a consumer has to wait for this.
    """
    deadline = time.monotonic() + timeout
    while not consumer.assignment():
        if time.monotonic() > deadline:
            raise TimeoutError("consumer was never assigned partitions")
        await asyncio.sleep(0.05)
    for tp in consumer.assignment():
        await consumer.position(tp)


class TopicTail:
    """
    Reads a topic from its current end, outside any consumer group.

        async with TopicTail(WHALE_ALERTS_TOPIC) as tail:
            ...produce...
            alert = await tail.next_json(lambda a: a["symbol"] == symbol)
    """

    def __init__(self, topic: str) -> None:
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )

    async def __aenter__(self) -> "TopicTail":
        await self._consumer.start()
        await wait_until_positioned(self._consumer)
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._consumer.stop()

    async def next_message(self, match: Callable[[Any], bool], timeout: float = 15.0):
        """The next record `match` accepts; raises TimeoutError after `timeout` seconds."""

        async def find():
            async for message in self._consumer:
                if match(message):
                    return message

        return await asyncio.wait_for(find(), timeout)

    async def next_json(self, match: Callable[[Dict[str, Any]], bool], timeout: float = 15.0) -> Dict[str, Any]:
        message = await self.next_message(lambda m: match(json.loads(m.value)), timeout)
        return json.loads(message.value)

    async def collect_json(self, match: Callable[[Dict[str, Any]], bool], quiet: float = 1.0) -> List[Dict[str, Any]]:
        """Every matching message until none at all arrives for `quiet` seconds."""
        found = []
        while True:
            batch = await self._consumer.getmany(timeout_ms=int(quiet * 1000))
            if not batch:
                return found
            for records in batch.values():
                for record in records:
                    value = json.loads(record.value)
                    if match(value):
                        found.append(value)


def parse_utc(text: str) -> datetime:
    """An ISO 8601 timestamp, "Z" suffix included, which fromisoformat only accepts from Python 3.11."""
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def wait_for(condition: Callable[[], Any], timeout: float = 30.0, interval: float = 0.2, message: str = ""):
    """Poll `condition` until it returns something truthy, and return that."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if result:
            return result
        if time.monotonic() > deadline:
            raise TimeoutError(message or f"condition not met within {timeout}s")
        time.sleep(interval)


async def async_wait_for(condition: Callable[[], Any], timeout: float = 30.0, interval: float = 0.05, message: str = ""):
    """wait_for, without blocking the event loop."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if result:
            return result
        if time.monotonic() > deadline:
            raise TimeoutError(message or f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


def golden_minutes(count: int, start: datetime, seed: int = 7) -> List[Tuple[int, str]]:
    """
    `count` consecutive final 1m candles from `start`: (Unix seconds, close),
    a seeded random walk around 97,000 with cent prices, the same every run.
    """
    rng = random.Random(seed)
    first = int(start.timestamp())
    price = Decimal("97000.00")
    minutes = []
    for i in range(count):
        price += Decimal(rng.randint(-2500, 2500)) / 100
        minutes.append((first + 60 * i, str(price)))
    return minutes


def math_js_ema(
    minutes: Sequence[Tuple[int, str]], timeframes: Mapping[str, int], lengths: Sequence[int]
) -> Dict[str, Dict[int, Dict[int, Optional[float]]]]:
    """
    The browser's EMA over `minutes` (final 1m candles, oldest first), from
    frontend/src/indicators/math.js itself, via tests/js/ema_reference.mjs:
    {timeframe: {length: {bar start: value or None}}}.
    """
    node = shutil.which("node")
    if node is None:
        import pytest

        pytest.skip("node not on PATH: the golden tests run frontend/src/indicators/math.js itself")
    request = {"minutes": [list(m) for m in minutes], "timeframes": dict(timeframes), "lengths": list(lengths)}
    result = subprocess.run(
        [node, str(EMA_REFERENCE_SCRIPT)], input=json.dumps(request), capture_output=True, text=True, check=True
    )
    out = json.loads(result.stdout)
    return {
        timeframe: {
            int(period): dict(zip(series["starts"], values)) for period, values in series["ema"].items()
        }
        for timeframe, series in out.items()
    }
