"""
Tests for streaming/jobs/futures_ratios.py, the long/short ratios'
consumer: parsing, then on a real broker into the real Postgres and Redis.
What the poller publishes lands in futures_ratios exactly; a repeat
changes nothing and pushes nothing; a revision changes just its row and
pushes just its period; offsets are committed only once Postgres has a
batch; Postgres being down holds the batch rather than losing it.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer

from config.settings import FUTURES_RATIOS_TOPIC, KAFKA_BOOTSTRAP_SERVERS

from jobs.futures_ratios import FuturesRatioConsumer, parse_ratio
from postgres_sink import FuturesRatioPostgresSink
from redis_sink import FuturesRatioRedisSink
from schema import FuturesRatio

from helpers import async_wait_for, wait_until_positioned

START = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)
PERIOD = timedelta(minutes=5)
STREAMING_DIR = Path(__file__).resolve().parents[2] / "streaming"


def ratio(symbol: str, kind: str, i: int, value: str = "1.25") -> FuturesRatio:
    return FuturesRatio(
        exchange="binance",
        symbol=symbol,
        event_time=START + i * PERIOD,
        kind=kind,
        ratio=Decimal(value),
        long_value=Decimal("0.5556"),
        short_value=Decimal("0.4444"),
    )


# --- parsing, no broker ------------------------------------------------------


def test_a_message_parses_into_a_row():
    row = parse_ratio(json.dumps(ratio("BTCUSDT", "top_positions", 1).model_dump(mode="json")).encode())
    assert row == {
        "symbol": "BTCUSDT",
        "period_start": START + PERIOD,
        "kind": "top_positions",
        "ratio": Decimal("1.25"),
        "long_value": Decimal("0.5556"),
        "short_value": Decimal("0.4444"),
    }


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b'{"event_type": "trade"}',
        b'{"event_type": "futures_ratio", "symbol": "X"}',
        json.dumps({**ratio("BTCUSDT", "taker_volume", 0).model_dump(mode="json"), "kind": "funding"}).encode(),
        json.dumps({**ratio("BTCUSDT", "taker_volume", 0).model_dump(mode="json"), "ratio": "NaN"}).encode(),
    ],
)
def test_anything_else_is_skipped(raw):
    assert parse_ratio(raw) is None


# --- on a real broker --------------------------------------------------------


async def skip_to_the_end(group_id: str) -> None:
    """Commit the topic's end for `group_id`, so a test's consumer starts after every earlier test's messages."""
    consumer = AIOKafkaConsumer(
        FUTURES_RATIOS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=group_id, enable_auto_commit=False
    )
    await consumer.start()
    try:
        await wait_until_positioned(consumer)
        await consumer.seek_to_end()
        await consumer.commit({tp: await consumer.position(tp) for tp in consumer.assignment()})
    finally:
        await consumer.stop()


class Running:
    """A FuturesRatioConsumer in its own group, run in the background."""

    def __init__(self, group_id: str, **kwargs) -> None:
        self.consumer = FuturesRatioConsumer(group_id=group_id, **kwargs)
        self.task = None

    async def __aenter__(self) -> FuturesRatioConsumer:
        await self.consumer.start()
        self.task = asyncio.create_task(self.consumer.run())
        return self.consumer

    async def __aexit__(self, *exc_info) -> None:
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.consumer.stop()


@pytest.fixture
async def group(kafka_topics, postgres_schema, symbol):
    group_id = f"test-futures-ratios-{symbol}"
    await skip_to_the_end(group_id)
    return group_id


@pytest.fixture
def updates(redis_client, symbol):
    """Every message published on the symbol's update channel, as a list that fills as they come."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"futures_ratio_updates:{symbol}")
    pubsub.get_message(timeout=1)  # the subscribe confirmation

    def read():
        received = []
        while (message := pubsub.get_message(timeout=0.2)) is not None:
            if message["type"] == "message":
                received.append(json.loads(message["data"]))
        return received

    yield read
    pubsub.close()


def stored(pg, symbol):
    return pg.execute(
        "SELECT period_start, kind, ratio, long_value, short_value FROM futures_ratios WHERE symbol = %s "
        "ORDER BY period_start, kind",
        (symbol,),
    ).fetchall()


async def test_published_ratios_land_in_postgres_and_their_span_is_pushed(group, trade_producer, pg, symbol, updates):
    kinds = ["global_accounts", "taker_volume"]
    await trade_producer.send_batch([ratio(symbol, kind, i) for i in range(3) for kind in kinds])
    async with Running(group):
        await async_wait_for(lambda: len(stored(pg, symbol)) == 6, timeout=30)
        await asyncio.sleep(0.5)

    rows = stored(pg, symbol)
    assert [(r["period_start"], r["kind"]) for r in rows] == [(START + i * PERIOD, k) for i in range(3) for k in kinds]
    assert all((r["ratio"], r["long_value"], r["short_value"]) == (Decimal("1.25"), Decimal("0.5556"), Decimal("0.4444")) for r in rows)
    pushed = updates()
    assert pushed[-1] == {
        "symbol": symbol,
        "first_period": START.isoformat(),
        "last_period": (START + 2 * PERIOD).isoformat(),
    }


async def test_a_repeat_changes_nothing_and_a_revision_only_its_period(group, trade_producer, pg, symbol, updates):
    await trade_producer.send_batch([ratio(symbol, "top_accounts", i) for i in range(4)])
    async with Running(group) as consumer:
        await async_wait_for(lambda: consumer.stored == 4, timeout=30)
        updates()

        # The same four again (a refetch), then one of them revised.
        await trade_producer.send_batch([ratio(symbol, "top_accounts", i) for i in range(4)])
        await trade_producer.send_batch([ratio(symbol, "top_accounts", 2, value="1.2500")])  # equal by value
        await asyncio.sleep(2)
        assert consumer.stored == 4 and updates() == []

        await trade_producer.send_batch([ratio(symbol, "top_accounts", 2, value="1.3")])
        await async_wait_for(lambda: consumer.stored == 5, timeout=30)
        await asyncio.sleep(0.5)

    assert [r["ratio"] for r in stored(pg, symbol)] == [Decimal("1.25"), Decimal("1.25"), Decimal("1.3"), Decimal("1.25")]
    assert updates() == [
        {"symbol": symbol, "first_period": (START + 2 * PERIOD).isoformat(), "last_period": (START + 2 * PERIOD).isoformat()}
    ]


async def test_offsets_are_committed_once_postgres_has_the_batch(group, trade_producer, pg, symbol):
    await trade_producer.send_batch([ratio(symbol, "taker_volume", i) for i in range(2)])
    async with Running(group):
        await async_wait_for(lambda: len(stored(pg, symbol)) == 2, timeout=30)
        await asyncio.sleep(0.5)

    # Restarted in the same group: nothing to re-read.
    async with Running(group) as again:
        await asyncio.sleep(2)
        assert again.stored == 0
    pg.execute("DELETE FROM futures_ratios WHERE symbol = %s", (symbol,))
    async with Running(group):
        await asyncio.sleep(2)
    assert stored(pg, symbol) == []


async def test_postgres_being_down_holds_the_batch_until_it_is_back(group, trade_producer, pg, symbol, fake_time):
    attempts = []

    def connect():
        attempts.append(1)
        if len(attempts) < 3:
            raise psycopg.OperationalError("connection refused (a test's stand-in)")
        return FuturesRatioPostgresSink()

    await trade_producer.send_batch([ratio(symbol, "global_accounts", 0)])
    async with Running(group, postgres=connect, sleep=fake_time.sleep):
        await async_wait_for(lambda: len(stored(pg, symbol)) == 1, timeout=30)
    assert len(attempts) == 3 and fake_time.sleeps == [1.0, 2.0]


async def test_a_malformed_message_is_skipped_without_holding_up_the_rest(group, trade_producer, pg, symbol):
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(FUTURES_RATIOS_TOPIC, b"{not json", key=symbol.encode())
    finally:
        await producer.stop()
    await trade_producer.send_batch([ratio(symbol, "top_positions", 0)])
    async with Running(group):
        await async_wait_for(lambda: len(stored(pg, symbol)) == 1, timeout=30)


def test_a_redis_sink_pushes_one_span_per_symbol(redis_client, symbol):
    other = symbol.replace("USDT", "XUSDT")
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"futures_ratio_updates:{symbol}", f"futures_ratio_updates:{other}")
    for _ in range(2):
        pubsub.get_message(timeout=1)
    FuturesRatioRedisSink().publish_changes(
        [(symbol, START + PERIOD), (other, START), (symbol, START), (symbol, START + 3 * PERIOD)]
    )
    received = []
    while (message := pubsub.get_message(timeout=1)) is not None:
        if message["type"] == "message":
            received.append(json.loads(message["data"]))
    pubsub.close()
    assert sorted(received, key=lambda m: m["symbol"]) == sorted(
        [
            {"symbol": symbol, "first_period": START.isoformat(), "last_period": (START + 3 * PERIOD).isoformat()},
            {"symbol": other, "first_period": START.isoformat(), "last_period": START.isoformat()},
        ],
        key=lambda m: m["symbol"],
    )


def test_refuses_the_replay_pipeline(test_services):
    env = {**os.environ, "PYTHONPATH": str(STREAMING_DIR.parent), "DSVIEW_PIPELINE": "replay"}
    result = subprocess.run(
        [sys.executable, "-m", "jobs.futures_ratios"], cwd=STREAMING_DIR, env=env, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0 and "live pipeline's only" in result.stderr
