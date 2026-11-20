"""
kafka_client.py

Kafka read side for the backend: the sole place backend/ touches
`aiokafka`, the way redis_client.py is for Redis and postgres_client.py
for Postgres. Reads the alerts.whales topic that
streaming/jobs/whale_detector.py publishes to, for routers/whales.py.

One consumer per WebSocket connection, with no consumer group: each
connection reads every partition for itself and commits nothing, so two
browser tabs both see every alert instead of splitting them, and nothing
is left behind in the broker when a tab closes. That's a Kafka connection
per open tab — fine at dev scale. If this ever serves many clients, swap
it for one shared consumer that fans out to every socket.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, List

from aiokafka import AIOKafkaConsumer, TopicPartition

from config.settings import KAFKA_BOOTSTRAP_SERVERS, WHALE_ALERTS_TOPIC

logger = logging.getLogger(__name__)

# How often to check whether the subscription has been given its partitions.
# A local check, no network: aiokafka fetches the topic's metadata itself.
ASSIGNMENT_POLL_SECONDS = 0.2


class WhaleAlertReader:
    """Reads whale alerts (as JSON strings) from alerts.whales: recent ones first, then live."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = WHALE_ALERTS_TOPIC,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic

    async def stream(self, backlog: int) -> AsyncIterator[str]:
        """
        Yield the last `backlog` alerts per partition, then every new one, until closed.

        The topic has one partition unless someone adds more, so in practice
        that's the last `backlog` alerts, oldest first. Raises a KafkaError
        if the broker can't be reached while starting up — at once if it
        refuses connections, or after aiokafka's 40 s request timeout if it
        goes away mid-way (the backlog's offset lookup is what waits). Once
        streaming, the consumer rides out broker outages on its own.
        """
        # Subscribing (even with no group) is what makes aiokafka track the
        # topic's metadata and hand this consumer all of its partitions,
        # including any added later. A bare consumer knows no partitions at
        # all: partitions_for_topic() stays None even for a topic that exists.
        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,  # no group: read everything, commit nothing (see module docstring)
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        await consumer.start()
        try:
            partitions = await self._wait_for_assignment(consumer)

            # Step back `backlog` messages from the end of each partition (or
            # to its beginning, if it holds fewer — retention may have dropped
            # older ones). An alert landing in the instant between assignment
            # and these seeks can arrive twice, which clients already dedupe.
            beginnings = await consumer.beginning_offsets(partitions)
            ends = await consumer.end_offsets(partitions)
            for tp in partitions:
                consumer.seek(tp, max(beginnings[tp], ends[tp] - backlog))

            async for message in consumer:
                yield message.value.decode("utf-8")
        finally:
            await _stop(consumer)

    async def _wait_for_assignment(self, consumer: AIOKafkaConsumer) -> List[TopicPartition]:
        """
        Return the partitions the subscription was assigned, once it has them.

        Normally immediate. If alerts.whales doesn't exist yet (no whale so
        far), the subscription's own metadata request creates it on a broker
        that auto-creates topics — Redpanda here, the same way the detector's
        first publish would — so this only waits where auto-creation is off,
        until something else creates the topic.
        """
        waiting_logged = False
        while not consumer.assignment():
            if not waiting_logged:
                logger.info("Waiting for topic '%s' to exist", self._topic)
                waiting_logged = True
            await asyncio.sleep(ASSIGNMENT_POLL_SECONDS)
        return sorted(consumer.assignment())


async def _stop(consumer: AIOKafkaConsumer) -> None:
    """
    consumer.stop(), minus a CancelledError aiokafka leaks when the broker is unreachable.

    aiokafka's fetcher.close() awaits the fetch requests it has just
    cancelled and lets their CancelledError escape. Raised from stream()'s
    finally, it would replace the error actually ending the stream (say,
    the KafkaTimeoutError above) and make the task look cancelled instead
    of failed. A real cancellation of this task is still re-raised — and so
    is everything on Python < 3.11, which can't tell the two apart.
    """
    try:
        await consumer.stop()
    except asyncio.CancelledError:
        task = asyncio.current_task()
        cancelling = getattr(task, "cancelling", None)  # Task.cancelling() is Python 3.11+
        if cancelling is None or cancelling():
            raise
        logger.debug("Ignored a CancelledError leaked by aiokafka's consumer.stop()")
