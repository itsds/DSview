"""
Tests for streaming/jobs/candle_aggregator.py: trades on Kafka in, candles
out in Redis (SET + PUBLISH) and Postgres (upsert), through real Spark.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from jobs.candle_aggregator import build_candle_query
from postgres_sink import CandlePostgresSink
from redis_sink import CandleRedisSink

from helpers import make_trade, wait_for

pytestmark = pytest.mark.spark

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def candle_query(spark, pg, redis_client, symbol):
    """candle_aggregator's query, running and past its first (empty) batch, so offsets are pinned."""
    redis_sink = CandleRedisSink()
    postgres_sink = CandlePostgresSink()
    query = build_candle_query(spark, redis_sink, postgres_sink)
    try:
        wait_for(lambda: query.lastProgress is not None, timeout=60, message="candle query never started")
        yield query
    finally:
        query.stop()
        postgres_sink.close()
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        redis_client.delete(f"candle:{symbol}:1m")


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
