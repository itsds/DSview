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
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

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
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "dsview-ingestion",
        acks: str = "all",
        linger_ms: int = 5,
    ) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._acks = acks
        self._linger_ms = linger_ms

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

        topic = kafka_topic_for(event)
        key = event.symbol.encode("utf-8")
        value = json.dumps(event.model_dump(mode="json"), default=_json_default).encode("utf-8")

        try:
            await self._producer.send_and_wait(topic, value=value, key=key)
        except KafkaError:
            logger.exception("Failed to publish event to topic '%s' (symbol=%s)", topic, event.symbol)
            raise
