"""
alert_publisher.py

Kafka write side for alerts: the one place in streaming/ that produces
to an alerts topic, the way ingestion/producer.py is the one place
ingestion/ produces. jobs/whale_detector.py's WhaleDetector never imports
this — it calls the on_whale callback it was constructed with, and the
job's _main() wires that callback to AlertPublisher.publish. That keeps
the detection logic runnable and testable without a broker, the same
reason ingestion's exchange clients take an on_event callback instead of
the producer.

Alerts go to a topic rather than straight to Telegram or the backend so
fan-out stays free: the Telegram bot, the backend's /ws/whales and
anything later each subscribe with their own consumer group, and the
detector never learns they exist.

Each alert is keyed by symbol, like every market event (see
ingestion/producer.py), so one symbol's alerts stay in order on one
partition.

Author: @DS
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping, Optional

from aiokafka import AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


class AlertPublisher:
    """Publishes alert dicts as JSON onto one Kafka topic, keyed by symbol."""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = "dsview-alerts",
    ) -> None:
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Connect to the broker. Call once before publish()."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks="all",
            # The producer retries on its own; idempotence stops a retry of a
            # send the broker already took from landing a duplicate alert.
            enable_idempotence=True,
        )
        await self._producer.start()
        logger.info("AlertPublisher producing to '%s' on %s", self._topic, self._bootstrap_servers)

    async def stop(self) -> None:
        """Flush and close the producer. Call once at shutdown."""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("AlertPublisher stopped")

    async def publish(self, alert: Mapping[str, Any]) -> None:
        """
        Publish one alert and wait for the broker to acknowledge it.

        Raises on failure (after the producer's own retries) and leaves
        what to do about it to the caller.
        """
        if self._producer is None:
            raise RuntimeError("AlertPublisher.start() must be called before publish()")

        await self._producer.send_and_wait(
            self._topic,
            value=json.dumps(dict(alert)).encode("utf-8"),
            key=str(alert["symbol"]).encode("utf-8"),
        )
