"""
ratio_topic.py

What `market.futures.ratios` already holds: the start of the newest period
of each (symbol, ratio) on the topic, which futures_main.py hands to the
poller as where to resume (futures_poller.py). The topic is the poller's
memory: nothing else to keep in step, and a period only counts once the
broker has it.

producer.py stays the one place ingestion writes to Kafka; this is the
one place it reads, and only its own topic's tail, outside any consumer
group (nothing is committed).

The tail is read backwards in chunks of CHUNK messages per partition, from
the end, until every (symbol, ratio) asked for has turned up, the
partition's beginning is reached, or MAX_SCAN messages have been read.
Every message for a symbol is on the partition its key (the symbol) hashes
to, in the order they were published, but other symbols share partitions
and a `--refetch-days` run can republish older periods after newer ones,
so the newest is the largest period start seen, not the last message. A
(symbol, ratio) not found has nothing to resume from: the poller starts
30 days back, which only costs requests.

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Iterable, List, Set, Tuple

from aiokafka import AIOKafkaConsumer, TopicPartition

from config.settings import FUTURES_RATIOS_TOPIC, KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

CHUNK = 2_000
MAX_SCAN = 200_000  # per partition: 30 days of four ratios for five symbols
ASSIGNMENT_TIMEOUT_SECONDS = 15.0
READ_TIMEOUT_SECONDS = 30.0  # for one chunk; a broker that stops answering mid-read

Key = Tuple[str, str]  # (symbol, ratio)


async def newest_periods(
    wanted: Iterable[Key],
    topic: str = FUTURES_RATIOS_TOPIC,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    chunk: int = CHUNK,
    max_scan: int = MAX_SCAN,
) -> Dict[Key, int]:
    """Per (symbol, ratio) in `wanted` found on `topic`, its newest period's start (Unix ms)."""
    wanted = set(wanted)
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=None,  # read only; commit nothing
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        partitions = await _assignment(consumer, topic)
        if not partitions:
            return {}
        beginnings = await consumer.beginning_offsets(partitions)
        ends = await consumer.end_offsets(partitions)
        found: Dict[Key, int] = {}
        for tp in partitions:
            await _scan(consumer, tp, beginnings[tp], ends[tp], wanted, found, chunk, max_scan)
        return found
    finally:
        await consumer.stop()


async def _assignment(consumer: AIOKafkaConsumer, topic: str) -> List[TopicPartition]:
    """
    The topic's partitions, once the subscription has them (see
    backend/kafka_client.py for why subscribing, not assign()). On a broker
    that doesn't create topics on request, a topic nobody has published to
    yet never arrives: nothing to resume from.
    """
    deadline = time.monotonic() + ASSIGNMENT_TIMEOUT_SECONDS
    while not consumer.assignment():
        if time.monotonic() > deadline:
            logger.warning("Topic '%s' has no partitions yet: nothing to resume from", topic)
            return []
        await asyncio.sleep(0.1)
    return sorted(consumer.assignment())


async def _scan(
    consumer: AIOKafkaConsumer,
    tp: TopicPartition,
    beginning: int,
    end: int,
    wanted: Set[Key],
    found: Dict[Key, int],
    chunk: int,
    max_scan: int,
) -> None:
    """Read `tp` backwards a chunk at a time from `end`, into `found`, until every wanted key is in it."""
    others = [other for other in consumer.assignment() if other != tp]
    consumer.pause(*others)
    try:
        high = end
        while high > beginning and end - high < max_scan and not wanted <= found.keys():
            low = max(beginning, high - chunk)
            consumer.seek(tp, low)
            await _read_until(consumer, tp, high, wanted, found)
            high = low
    finally:
        consumer.resume(*others)


async def _read_until(consumer: AIOKafkaConsumer, tp: TopicPartition, stop: int, wanted: Set[Key], found: Dict[Key, int]) -> None:
    """Read `tp` from its position up to offset `stop` (exclusive), keeping the newest period per wanted key."""
    deadline = time.monotonic() + READ_TIMEOUT_SECONDS
    while await consumer.position(tp) < stop:
        if time.monotonic() > deadline:
            logger.warning(
                "Reading %s back timed out at offset %d of %d; resuming from what was read",
                tp,
                await consumer.position(tp),
                stop,
            )
            return
        batch = await consumer.getmany(tp, timeout_ms=1000, max_records=CHUNK)
        for record in batch.get(tp, []):
            if record.offset >= stop:
                return
            key, start = _period(record.value)
            if key in wanted and start > found.get(key, -1):
                found[key] = start


def _period(value: bytes) -> Tuple[Key | None, int]:
    """(symbol, ratio) and period start (Unix ms) of one message, or (None, -1) if it isn't a ratio."""
    try:
        message = json.loads(value)
        start = datetime.fromisoformat(message["event_time"].replace("Z", "+00:00"))
        return (message["symbol"], message["kind"]), int(start.timestamp() * 1000)
    except (ValueError, KeyError, TypeError, AttributeError):
        return None, -1
