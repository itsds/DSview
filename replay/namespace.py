"""
namespace.py

ReplayNamespace: empties the replay pipeline before a run, and seeds it
with the history before the run's start. One replay at a time, and starting
one clears the last (Phase 4a's kickoff decision 1): nothing a run leaves
behind may leak into the next one's chart.

**reset()** empties everything the replay pipeline owns (config/settings.py's
Pipeline), and nothing else:

- Kafka: its topics are truncated to their end (DeleteRecords), creating
  any that's missing. A consumer reading from the earliest offset (every
  replay job does: Pipeline.starting_offsets) then starts at the new run,
  and the replay whale detector's committed offsets, which point at the
  old run's end, line up with it. If the broker refuses DeleteRecords, the
  topic is deleted and created again instead.
- Postgres: its schema's tables are truncated.
- Redis: every key under its prefix (`replay:*`) is deleted. The replay's
  controls live outside the prefix (REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY).
- Parquet: its data root's checkpoints are deleted, so the indicator job
  starts afresh rather than resume the last run's EMAs and offsets.

**seed()** copies live's `candles_1m` before the start into replay's, so the
replay's indicators warm up on the same history live's had (EMA 200 on 1D
needs 200 days), and the chart has bars before the first replayed one; and
the footprint's last SEED_FOOTPRINT before the start, for the bars the
chart first shows.

Refuses to work on the live pipeline: emptying it would delete DSView's
history.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import psycopg
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic, RecordsToDelete
from aiokafka.errors import KafkaError, TopicAlreadyExistsError
from psycopg import sql

from config.settings import KAFKA_BOOTSTRAP_SERVERS, LIVE_PIPELINE, REDIS_HOST, REDIS_PORT, REPLAY_PIPELINE, Pipeline

logger = logging.getLogger(__name__)

TABLES = ("candles_1m", "candle_indicators", "candle_revisions", "footprint_1m")
SEED_FOOTPRINT = timedelta(days=1)
TOPIC_RECREATE_ATTEMPTS = 30


class ReplayNamespace:
    def __init__(
        self,
        pipeline: Pipeline = REPLAY_PIPELINE,
        source: Pipeline = LIVE_PIPELINE,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        redis_host: str = REDIS_HOST,
        redis_port: int = REDIS_PORT,
        data_root: Optional[str] = None,  # the pipeline's own by default; tests use a temporary one
    ) -> None:
        if pipeline.is_live:
            raise ValueError("refusing to empty the live pipeline")
        self._pipeline = pipeline
        self._data_root = data_root or pipeline.data_root
        self._source = source
        self._bootstrap_servers = bootstrap_servers
        self._redis_host = redis_host
        self._redis_port = redis_port

    @property
    def topics(self) -> List[str]:
        p = self._pipeline
        return [p.trades_topic, p.depth_topic, p.whale_alerts_topic, p.candles_topic]

    async def reset(self) -> None:
        await self._reset_topics()
        await asyncio.to_thread(self._reset_tables)
        await self._reset_redis()
        shutil.rmtree(Path(self._data_root) / "_checkpoints", ignore_errors=True)

    async def seed(self, symbol: str, start: datetime) -> Dict[str, int]:
        """Copy `symbol`'s history before `start` from the source pipeline; returns rows copied per table."""
        return await asyncio.to_thread(self._seed, symbol, start)

    # --- Kafka -----------------------------------------------------------------

    async def _reset_topics(self) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            missing = [t for t in self.topics if t not in existing]
            if missing:
                await _create(admin, missing)
            present = [t for t in self.topics if t in existing]
            if present:
                ends = await self._end_offsets(await _partitions(admin, present))
                nonempty = {tp: RecordsToDelete(before_offset=offset) for tp, offset in ends.items() if offset > 0}
                if nonempty:
                    try:
                        await admin.delete_records(nonempty, timeout_ms=30_000)
                    except KafkaError as error:
                        logger.warning("DeleteRecords refused (%r); deleting and recreating the replay topics", error)
                        await self._recreate(admin, sorted({tp.topic for tp in nonempty}))
        finally:
            await admin.close()

    async def _end_offsets(self, partitions: List[TopicPartition]) -> Dict[TopicPartition, int]:
        consumer = AIOKafkaConsumer(bootstrap_servers=self._bootstrap_servers, enable_auto_commit=False)
        await consumer.start()
        try:
            return await consumer.end_offsets(partitions)
        finally:
            await consumer.stop()

    async def _recreate(self, admin: AIOKafkaAdminClient, topics: List[str]) -> None:
        await admin.delete_topics(topics)
        for _ in range(TOPIC_RECREATE_ATTEMPTS):  # deletion completes in the background
            try:
                await _create(admin, topics, quiet=False)
                return
            except TopicAlreadyExistsError:
                await asyncio.sleep(1)
        raise RuntimeError(f"replay topics {topics} still exist {TOPIC_RECREATE_ATTEMPTS} s after deleting them")

    # --- Postgres --------------------------------------------------------------

    def _reset_tables(self) -> None:
        with psycopg.connect(self._pipeline.postgres_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(map(sql.Identifier, TABLES))))

    def _seed(self, symbol: str, start: datetime) -> Dict[str, int]:
        target, source = self._pipeline.postgres_schema, self._source.postgres_schema
        copied = {}
        with psycopg.connect(self._source.postgres_dsn) as conn:  # one transaction: both copies or neither
            copied["candles_1m"] = conn.execute(
                sql.SQL("INSERT INTO {}.candles_1m SELECT * FROM {}.candles_1m WHERE symbol = %s AND window_start < %s").format(
                    sql.Identifier(target), sql.Identifier(source)
                ),
                (symbol, start),
            ).rowcount
            copied["footprint_1m"] = conn.execute(
                sql.SQL(
                    "INSERT INTO {}.footprint_1m SELECT * FROM {}.footprint_1m"
                    " WHERE symbol = %s AND window_start >= %s AND window_start < %s"
                ).format(sql.Identifier(target), sql.Identifier(source)),
                (symbol, start - SEED_FOOTPRINT, start),
            ).rowcount
        return copied

    # --- Redis -----------------------------------------------------------------

    async def _reset_redis(self) -> None:
        client = redis.Redis(host=self._redis_host, port=self._redis_port)
        try:
            batch = []
            async for key in client.scan_iter(match=f"{self._pipeline.redis_prefix}*", count=1000):
                batch.append(key)
                if len(batch) >= 1000:
                    await client.delete(*batch)
                    batch = []
            if batch:
                await client.delete(*batch)
        finally:
            await client.aclose()


async def _create(admin: AIOKafkaAdminClient, topics: List[str], quiet: bool = True) -> None:
    # One partition each, as live's: a symbol's trades and candles stay in order.
    try:
        await admin.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics])
    except TopicAlreadyExistsError:
        if not quiet:
            raise


async def _partitions(admin: AIOKafkaAdminClient, topics: List[str]) -> List[TopicPartition]:
    return [
        TopicPartition(described["topic"], partition["partition"])
        for described in await admin.describe_topics(topics)
        for partition in described["partitions"]
    ]
