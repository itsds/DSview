"""Tests for replay/namespace.py over real Kafka, Redis and Postgres: a reset empties replay's things and nothing of live's."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, LIVE_PIPELINE, REPLAY_PIPELINE, REPLAY_STATUS_KEY

from namespace import ReplayNamespace

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def namespace(test_services, postgres_schema, tmp_path):
    return ReplayNamespace(data_root=str(tmp_path / "replay"))


@pytest.fixture
def replay_db(postgres_schema):
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn, autocommit=True) as conn:
        yield conn


async def _earliest(topic: str):
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=None, enable_auto_commit=False, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        batches = await consumer.getmany(timeout_ms=2000)
        return [m.value for records in batches.values() for m in records]
    finally:
        await consumer.stop()


def test_the_live_pipeline_is_never_emptied():
    with pytest.raises(ValueError, match="refusing to empty the live pipeline"):
        ReplayNamespace(pipeline=LIVE_PIPELINE)


async def test_a_reset_starts_the_replays_topics_afresh(namespace):
    await namespace.reset()  # creates them if missing
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        for i in range(3):
            await producer.send_and_wait(REPLAY_PIPELINE.trades_topic, b"last run %d" % i)
        await namespace.reset()
        await producer.send_and_wait(REPLAY_PIPELINE.trades_topic, b"this run")
    finally:
        await producer.stop()
    assert await _earliest(REPLAY_PIPELINE.trades_topic) == [b"this run"]


async def test_a_reset_empties_replays_tables_keys_and_checkpoints_only(namespace, pg, replay_db, redis_client, symbol, tmp_path):
    for conn in (pg, replay_db):
        conn.execute("INSERT INTO candles_1m VALUES (%s, %s, %s, 1, 1, 1, 1, 1, 1)", (symbol, BASE, BASE + timedelta(minutes=1)))
    redis_client.set(f"candle:{symbol}:1m", "live")
    redis_client.set(f"replay:candle:{symbol}:1m", "replay")
    redis_client.set(REPLAY_STATUS_KEY, "{}", ex=30)
    checkpoint = tmp_path / "replay" / "_checkpoints" / "indicators"
    checkpoint.mkdir(parents=True)
    try:
        await namespace.reset()

        assert replay_db.execute("SELECT count(*) FROM candles_1m").fetchone()[0] == 0
        assert pg.execute("SELECT count(*) AS n FROM candles_1m WHERE symbol = %s", (symbol,)).fetchone()["n"] == 1
        assert redis_client.get(f"replay:candle:{symbol}:1m") is None
        assert redis_client.get(f"candle:{symbol}:1m") == "live"
        assert redis_client.get(REPLAY_STATUS_KEY) == "{}"  # the controls live outside the prefix
        assert not checkpoint.exists()
    finally:
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        redis_client.delete(f"candle:{symbol}:1m", REPLAY_STATUS_KEY)


async def test_seeding_copies_live_history_before_the_start_and_a_day_of_footprint(namespace, pg, replay_db, symbol):
    start = BASE + timedelta(days=3)
    for days in (0, 2, 3):  # 3 days before the start, 1 day before, and the start itself
        minute = BASE + timedelta(days=days)
        pg.execute("INSERT INTO candles_1m VALUES (%s, %s, %s, 1, 1, 1, 1, 1, 1)", (symbol, minute, minute + timedelta(minutes=1)))
        pg.execute("INSERT INTO footprint_1m VALUES (%s, %s, 97000, 1, 1)", (symbol, minute))
    try:
        await namespace.reset()
        copied = await namespace.seed(symbol, start)

        assert copied == {"candles_1m": 2, "footprint_1m": 1}
        candles = [r[0] for r in replay_db.execute("SELECT window_start FROM candles_1m WHERE symbol = %s ORDER BY 1", (symbol,))]
        assert candles == [BASE, BASE + timedelta(days=2)]
        footprint = [r[0] for r in replay_db.execute("SELECT window_start FROM footprint_1m WHERE symbol = %s", (symbol,))]
        assert footprint == [BASE + timedelta(days=2)]
        assert pg.execute("SELECT count(*) AS n FROM candles_1m WHERE symbol = %s", (symbol,)).fetchone()["n"] == 3
    finally:
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        pg.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))
        replay_db.execute("TRUNCATE candles_1m, footprint_1m")
