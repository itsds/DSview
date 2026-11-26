"""
producer.py

Thin async wrapper around AIOKafkaProducer.

This is the sole boundary between the ingestion layer (base_client.py,
binance_client.py, ...) and Kafka/Redpanda. Exchange clients never touch
this class directly — they call the `on_event` callback they were given
at construction time, and whatever owns the wiring (e.g. main.py) passes
`MarketDataProducer.send` as that callback. That keeps ingestion testable
without a broker and lets the producer's internals change independently.

Author: @DS
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Sequence

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from config.settings import KAFKA_BOOTSTRAP_SERVERS, PIPELINE, Pipeline

from schema import MarketEvent, kafka_topic_for

logger = logging.getLogger(__name__)


def _json_default(value):
    """Handle types json.dumps doesn't know about out of the box."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class MarketDataProducer:
    """
    Publishes normalized MarketEvents onto their destination Kafka topics.

    Usage:
        producer = MarketDataProducer(bootstrap_servers="localhost:9092")
        await producer.start()
        ...
        await producer.send(event)
        ...
        await producer.stop()
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = "dsview-ingestion",
        acks: str = "all",
        linger_ms: int = 5,
        pipeline: Pipeline = PIPELINE,
    ) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._acks = acks
        self._linger_ms = linger_ms
        self._pipeline = pipeline  # whose topics events go to (see schema.kafka_topic_for)

    async def start(self) -> None:
        """Initialize and start the underlying Kafka client. Call once at app startup."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks=self._acks,
            linger_ms=self._linger_ms,
            value_serializer=lambda v: v,  # we pre-serialize to bytes ourselves in send()
            key_serializer=lambda k: k,
        )
        await self._producer.start()
        logger.info("MarketDataProducer connected to %s", self._bootstrap_servers)

    async def stop(self) -> None:
        """Flush and close the underlying Kafka client. Call once at app shutdown."""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("MarketDataProducer stopped")

    async def send(self, event: MarketEvent) -> None:
        """
        Serialize and publish a single normalized event.

        Partition key is the trading symbol so all events for a given
        instrument land on the same partition, preserving per-symbol
        ordering for downstream windowed aggregations in Spark.
        """
        if self._producer is None:
            raise RuntimeError("MarketDataProducer.start() must be called before send()")

        topic, key, value = self._record(event)

        try:
            await self._producer.send_and_wait(topic, value=value, key=key)
        except KafkaError:
            logger.exception("Failed to publish event to topic '%s' (symbol=%s)", topic, event.symbol)
            raise

    async def send_batch(self, events: Sequence[MarketEvent]) -> None:
        """
        Publish `events` in order and return once the broker has them all.

        For a feed that already has many events in hand (replay/): each
        send() waits for its acknowledgement before the next one goes, which
        caps a sender at a few hundred events a second. Here they are all
        queued first and travel together in the client's batches. Order per
        partition is kept, as with send() (aiokafka sends one batch per
        partition at a time unless asked otherwise).
        """
        if self._producer is None:
            raise RuntimeError("MarketDataProducer.start() must be called before send_batch()")

        deliveries = []
        for event in events:
            topic, key, value = self._record(event)
            deliveries.append(await self._producer.send(topic, value=value, key=key))
        try:
            for delivery in deliveries:
                await delivery
        except KafkaError:
            logger.exception("Failed to publish a batch of %d events", len(events))
            raise

    def _record(self, event: MarketEvent) -> tuple:
        """The (topic, key, value) `event` is published as."""
        topic = kafka_topic_for(event, self._pipeline)
        key = event.symbol.encode("utf-8")
        value = json.dumps(event.model_dump(mode="json"), default=_json_default).encode("utf-8")
        return topic, key, value
