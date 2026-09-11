"""
whale_detector.py

Per-trade whale detector — feature roadmap milestone 1.

Deliberately a plain asyncio Kafka/Redpanda consumer, not a Spark
Structured Streaming job: flagging a single trade against a size
threshold needs no windowing or cross-event state, so paying Spark's
startup and resource cost here would be pure overhead. The windowed
jobs (candle_aggregator.py, indicators.py, footprint.py) are where
Spark earns its keep.

Consumes the normalized trade events ingestion/producer.py already
published to `market.trades.raw` and logs any trade whose notional
value (price * quantity) meets or exceeds WHALE_THRESHOLD_USD. Reads
the raw JSON payload directly rather than importing
ingestion/schema.py's pydantic models — once an event is on the wire
it's just JSON, and a consumer shouldn't need to share Python types
with a producer across a Kafka topic boundary.

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from decimal import Decimal, InvalidOperation
from typing import Optional

from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TRADES_TOPIC = "market.trades.raw"
CONSUMER_GROUP_ID = "dsview-whale-detector"
WHALE_THRESHOLD_USD = Decimal("100000")


class WhaleDetector:
    """Consumes normalized trades and flags ones crossing WHALE_THRESHOLD_USD."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = TRADES_TOPIC,
        group_id: str = CONSUMER_GROUP_ID,
        threshold_usd: Decimal = WHALE_THRESHOLD_USD,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._threshold_usd = threshold_usd
        self._consumer: Optional[AIOKafkaConsumer] = None

    async def start(self) -> None:
        """Connect and subscribe. Call once before run()."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda v: v,  # decode raw bytes ourselves in _handle_message
            auto_offset_reset="latest",
        )
        await self._consumer.start()
        logger.info(
            "WhaleDetector consuming '%s' from %s (group=%s, threshold=$%s)",
            self._topic,
            self._bootstrap_servers,
            self._group_id,
            self._threshold_usd,
        )

    async def stop(self) -> None:
        """Close the underlying consumer. Call once at shutdown."""
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("WhaleDetector stopped")

    async def run(self) -> None:
        """Consume until cancelled."""
        if self._consumer is None:
            raise RuntimeError("WhaleDetector.start() must be called before run()")

        async for message in self._consumer:
            self._handle_message(message.value)

    def _handle_message(self, raw_value: bytes) -> None:
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            logger.exception("Failed to decode trade message, skipping")
            return

        if payload.get("event_type") != "trade":
            return  # depth updates carry no trade size, nothing to check

        try:
            notional = Decimal(str(payload["price"])) * Decimal(str(payload["quantity"]))
        except (KeyError, InvalidOperation):
            logger.exception("Malformed trade payload, skipping: %s", payload)
            return

        if notional >= self._threshold_usd:
            # TODO(@DS): push to alerts/telegram_bot.py once it exists;
            # logging is the only sink for now.
            logger.warning(
                "WHALE TRADE: %s %s %s @ %s (notional=$%.2f, side=%s)",
                payload.get("exchange"),
                payload.get("symbol"),
                payload.get("quantity"),
                payload.get("price"),
                notional,
                payload.get("side"),
            )


async def _main() -> None:
    detector = WhaleDetector()
    await detector.start()

    stop_event = asyncio.Event()

    def _handle_shutdown_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    run_task = asyncio.create_task(detector.run())

    await stop_event.wait()

    logger.info("Stopping whale detector...")
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)
    await detector.stop()
    logger.info("Whale detector stopped cleanly")


if __name__ == "__main__":
    asyncio.run(_main())
