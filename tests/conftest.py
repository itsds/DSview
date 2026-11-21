"""
conftest.py

Shared fixtures for DSView's test suite. Run it with scripts/test.sh,
which starts throwaway Kafka, Redis and Postgres (or uses the ones CI
provides) and points the suite at them through DSVIEW_* variables.

The suite runs against real services, not mocks: most of what can break
here (offsets, consumer groups, pub/sub, upserts, Spark's Kafka source)
lives in the services themselves.

Isolation:

- **From your dev stack.** Every fixture that touches a service first
  checks that DSVIEW_KAFKA_BOOTSTRAP_SERVERS, DSVIEW_REDIS_PORT and
  DSVIEW_POSTGRES_DSN are set, and fails otherwise. Without them
  config/settings.py falls back to the docker-compose.yml dev stack, and a
  test's synthetic whales would land on the real alerts.whales topic — the
  right panel would show them, and the Telegram bot would send them to
  your phone. Pure unit tests need no services and run anywhere.
- **From each other.** Topic names are fixed (config/settings.py), so
  tests share topics; each test trades a symbol of its own (the `symbol`
  fixture) and only looks at that symbol's messages, rows and keys.

Author: @DS
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import Iterator

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_ENV = ("DSVIEW_KAFKA_BOOTSTRAP_SERVERS", "DSVIEW_REDIS_PORT", "DSVIEW_POSTGRES_DSN")


@pytest.fixture(scope="session")
def test_services() -> None:
    """Fail unless the DSVIEW_* variables point the suite at test services."""
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.fail(
            f"{', '.join(missing)} not set. Run the suite with ./scripts/test.sh, which "
            "starts throwaway Kafka, Redis and Postgres for it; the defaults would point it "
            "at your dev stack (and send its test whales to your phone).",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def kafka_topics(test_services) -> None:
    """Create the pipeline's topics if missing: Spark's Kafka source fails on a topic that doesn't exist."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    from config.settings import (
        CANDLES_TOPIC,
        DEPTH_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        TRADES_TOPIC,
        WHALE_ALERTS_TOPIC,
    )

    async def create() -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            wanted = [
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
                for topic in (TRADES_TOPIC, DEPTH_TOPIC, WHALE_ALERTS_TOPIC, CANDLES_TOPIC)
                if topic not in existing
            ]
            if wanted:
                try:
                    await admin.create_topics(wanted)
                except TopicAlreadyExistsError:
                    pass  # created meanwhile (auto-creation): fine
        finally:
            await admin.close()

    asyncio.run(create())


@pytest.fixture
def symbol() -> str:
    """A symbol no other test uses, so a test only ever sees its own trades, candles and alerts."""
    return f"T{uuid.uuid4().hex[:8].upper()}USDT"


@pytest.fixture
async def trade_producer(kafka_topics):
    """Ingestion's real MarketDataProducer, started."""
    from producer import MarketDataProducer

    producer = MarketDataProducer()
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@pytest.fixture(scope="session")
def postgres_schema(test_services) -> None:
    """Apply storage/ddl/ once per run, through storage/init_db.py itself."""
    import init_db

    init_db.main()


@pytest.fixture
def pg(postgres_schema):
    """A psycopg connection (autocommit, dict rows) to the test database."""
    import psycopg
    from psycopg.rows import dict_row

    from config.settings import POSTGRES_DSN

    with psycopg.connect(POSTGRES_DSN, autocommit=True, row_factory=dict_row) as conn:
        yield conn


@pytest.fixture
def redis_client(test_services):
    """A plain (sync) Redis client on the test Redis."""
    import redis

    from config.settings import REDIS_HOST, REDIS_PORT

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="session")
def spark(kafka_topics):
    """One local SparkSession for every Spark test, built by streaming/spark_session.py."""
    from spark_session import get_spark_session

    # get_spark_session() also points Spark's Python workers (pandas UDFs, and
    # the tests' createDataFrame) at this interpreter.
    session = get_spark_session("dsview-tests")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def local_timezone(monkeypatch) -> Iterator[callable]:
    """Set this process's local timezone, the way a laptop outside UTC has it; restored afterwards."""

    def set_timezone(name: str) -> None:
        monkeypatch.setenv("TZ", name)
        time.tzset()

    yield set_timezone
    monkeypatch.undo()
    time.tzset()
