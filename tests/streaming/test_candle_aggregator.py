"""
Tests for streaming/jobs/candle_aggregator.py: trades on Kafka in, candles
out in Redis (SET + PUBLISH), Postgres (upsert) and the market.candles.1m
topic, through real Spark.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.settings import CANDLES_TOPIC

from helpers import TopicTail, make_trade, wait_for

pytestmark = pytest.mark.spark

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


def _at(seconds: int) -> datetime:
    return BASE + timedelta(seconds=seconds)


async def test_trades_become_utc_candles_in_postgres_and_redis(
    candle_query, trade_producer, pg, redis_client, symbol, local_timezone
):
    # The dev laptop runs in IST; collect() hands back naive local times
    # there, which once stored every candle 5 h 30 m in the future.
    local_timezone("Asia/Kolkata")
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"candle_updates:{symbol}:1m")

    for seconds, price, quantity in [(5, "100", "1"), (20, "105", "2"), (40, "98", "1"), (55, "101", "0.5")]:
        await trade_producer.send(make_trade(symbol, price, quantity, event_time=_at(seconds)))

    row = wait_for(
        lambda: pg.execute(
            "SELECT * FROM candles_1m WHERE symbol = %s AND trade_count = 4", (symbol,)
        ).fetchone(),
        timeout=60,
        message="candle never reached Postgres",
    )
    assert row["window_start"] == BASE
    assert row["window_end"] == BASE + timedelta(minutes=1)
    assert (row["open"], row["high"], row["low"], row["close"]) == (
        Decimal("100"), Decimal("105"), Decimal("98"), Decimal("101"),
    )
    assert row["volume"] == Decimal("4.5")

    cached = json.loads(redis_client.get(f"candle:{symbol}:1m"))
    assert datetime.fromisoformat(cached["window_start"]) == BASE
    assert redis_client.ttl(f"candle:{symbol}:1m") > 0

    published = [json.loads(m["data"]) for m in _drain(pubsub) if m["type"] == "message"]
    assert published, "nothing was published for live subscribers"
    assert published[-1]["trade_count"] == 4
    pubsub.close()


async def test_newest_window_wins_and_updates_never_go_back_in_time(
    candle_query, trade_producer, pg, redis_client, symbol
):
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"candle_updates:{symbol}:1m")

    # Two windows, sent together so they usually share a micro-batch.
    for seconds, price in [(10, "100"), (70, "200"), (20, "101"), (80, "201")]:
        await trade_producer.send(make_trade(symbol, price, "1", event_time=_at(seconds)))

    wait_for(
        lambda: pg.execute(
            "SELECT count(*) AS n FROM candles_1m WHERE symbol = %s AND trade_count = 2", (symbol,)
        ).fetchone()["n"] == 2,
        timeout=60,
        message="both candles never reached Postgres",
    )
    cached = json.loads(redis_client.get(f"candle:{symbol}:1m"))
    assert datetime.fromisoformat(cached["window_start"]) == BASE + timedelta(minutes=1)

    starts = [json.loads(m["data"])["window_start"] for m in _drain(pubsub) if m["type"] == "message"]
    assert starts == sorted(starts)
    pubsub.close()


def _drain(pubsub):
    messages = []
    while (message := pubsub.get_message(timeout=0.5)) is not None:
        messages.append(message)
    return messages


async def test_every_candle_update_is_published_to_the_candle_topic_in_order(
    candle_query, trade_producer, symbol, local_timezone
):
    local_timezone("Asia/Kolkata")  # the topic's times are UTC whatever the laptop's zone
    async with TopicTail(CANDLES_TOPIC) as tail:
        for seconds, price in [(10, "100"), (70, "200"), (20, "101"), (80, "201")]:
            await trade_producer.send(make_trade(symbol, price, "1.5", event_time=_at(seconds)))

        messages = []
        while {m["window_start"] for _, m in messages if m["trade_count"] == 2} != {
            "2026-11-20T10:00:00+00:00",
            "2026-11-20T10:01:00+00:00",
        }:
            message = await tail.next_message(lambda m: m.key == symbol.encode(), timeout=60)
            messages.append((message.key, json.loads(message.value)))

    first = next(m for _, m in messages if m["window_start"] == "2026-11-20T10:00:00+00:00" and m["trade_count"] == 2)
    assert first == {
        "symbol": symbol,
        "window_start": "2026-11-20T10:00:00+00:00",
        "window_end": "2026-11-20T10:01:00+00:00",
        "open": "100.000000000000000000",
        "high": "101.000000000000000000",
        "low": "100.000000000000000000",
        "close": "101.000000000000000000",
        "volume": "3.000000000000000000",
        "trade_count": 2,
    }
    # Within each micro-batch oldest window first, so time never runs backwards
    # except where a later batch revises a window again.
    starts = [m["window_start"] for _, m in messages]
    assert starts.index("2026-11-20T10:01:00+00:00") > starts.index("2026-11-20T10:00:00+00:00")
