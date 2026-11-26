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
published to `market.trades.raw`, and for every trade whose notional
value (price * quantity) meets or exceeds WHALE_THRESHOLD_USD, logs it
and hands an alert to the on_whale callback — which _main() wires to
alert_publisher.py, publishing it to `alerts.whales` (WHALE_ALERTS_TOPIC)
keyed by symbol. Reads the raw JSON payload directly rather than
importing ingestion/schema.py's pydantic models — once an event is on
the wire it's just JSON, and a consumer shouldn't need to share Python
types with a producer across a Kafka topic boundary.

Each alert on `alerts.whales` is one JSON object:

    {"event_type": "whale_trade", "exchange": "binance", "symbol": "BTCUSDT",
     "trade_id": "...", "side": "buy" | "sell",
     "price": "97000.1", "quantity": "2.5",   # exactly as the trade carried them
     "notional": "242500.25", "threshold": "100000",
     "event_time": "...",                     # when the trade happened (exchange clock)
     "detected_at": "..."}                    # when this detector flagged it (UTC)

Decimals are strings, like every other payload in the pipeline, so no
consumer loses precision to a float.

Shutdown is bounded: on Ctrl+C or SIGTERM the consumer and the publisher
each get SHUTDOWN_TIMEOUT_SECONDS to close. With the broker down,
aiokafka's consumer.stop() otherwise never returns — it commits offsets
and leaves the group first, and retries both until the broker answers.
Giving up only skips that last commit, so a restart re-reads at most the
last few seconds of trades (the repeats delivery already allows for).

Delivery is best-effort, not exactly-once: each alert is published and
acknowledged before the next trade is read, but the consumer
auto-commits its offsets. So a publish that still fails after the
producer's own retries is logged, alert included, and skipped; and a
crash can re-read the last few seconds of trades and repeat their
alerts. Consumers that mind duplicates dedupe on (exchange, trade_id).

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, Optional

from aiokafka import AIOKafkaConsumer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    PIPELINE,
    TRADES_TOPIC,
    WHALE_ALERTS_TOPIC,
    WHALE_THRESHOLD_USD,
)

from alert_publisher import AlertPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Live's group; another pipeline's detector uses its own (replay.dsview-whale-detector).
CONSUMER_GROUP_ID = PIPELINE.consumer_group("dsview-whale-detector")
ALERT_EVENT_TYPE = "whale_trade"

# Per client (consumer, then publisher). With the broker up each closes in
# well under a second; this only ever runs out when it's unreachable.
SHUTDOWN_TIMEOUT_SECONDS = 5.0

WhaleAlert = Dict[str, Any]
OnWhale = Callable[[WhaleAlert], Awaitable[None]]


def _decimal_str(value: Decimal) -> str:
    """Plain notation, no trailing zeros: never "1.94E+5" or "194000.2000000000000000"."""
    return format(value.normalize(), "f")


class WhaleDetector:
    """Consumes normalized trades and flags ones crossing WHALE_THRESHOLD_USD."""

    def __init__(
        self,
        on_whale: OnWhale,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = TRADES_TOPIC,
        group_id: str = CONSUMER_GROUP_ID,
        threshold_usd: Decimal = WHALE_THRESHOLD_USD,
    ) -> None:
        self._on_whale = on_whale
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
            # With no committed offset: live from now on, replay from the run's start.
            auto_offset_reset=PIPELINE.starting_offsets,
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
            alert = self._handle_message(message.value)
            if alert is None:
                continue
            try:
                await self._on_whale(alert)
            except Exception:
                # The alert is already in the log (see _handle_message); one
                # failed hand-off mustn't stop whale detection for good.
                logger.exception("Failed to deliver whale alert, skipping: %s", alert)

    def _handle_message(self, raw_value: bytes) -> Optional[WhaleAlert]:
        """Return a whale alert if this message is a trade at or over the threshold, else None."""
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            logger.exception("Failed to decode trade message, skipping")
            return None

        if payload.get("event_type") != "trade":
            return None  # depth updates carry no trade size, nothing to check

        try:
            symbol = payload["symbol"]  # required: it's the alert's partition key
            notional = Decimal(str(payload["price"])) * Decimal(str(payload["quantity"]))
        except (KeyError, InvalidOperation):
            logger.exception("Malformed trade payload, skipping: %s", payload)
            return None

        if notional < self._threshold_usd:
            return None

        logger.warning(
            "WHALE TRADE: %s %s %s @ %s (notional=$%.2f, side=%s)",
            payload.get("exchange"),
            symbol,
            payload.get("quantity"),
            payload.get("price"),
            notional,
            payload.get("side"),
        )
        return {
            "event_type": ALERT_EVENT_TYPE,
            "exchange": payload.get("exchange"),
            "symbol": symbol,
            "trade_id": payload.get("trade_id"),
            "side": payload.get("side"),
            "price": str(payload["price"]),
            "quantity": str(payload["quantity"]),
            "notional": _decimal_str(notional),
            "threshold": _decimal_str(self._threshold_usd),
            "event_time": payload.get("event_time"),
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }


async def stop_within(
    stop: Awaitable[None], what: str, timeout: float = SHUTDOWN_TIMEOUT_SECONDS, verb: str = "close"
) -> bool:
    """
    Await `stop` for at most `timeout` seconds; True if it finished cleanly.

    For closing a Kafka client at shutdown, which waits on the broker
    indefinitely when it's down (see the module docstring). Past the time
    limit `stop` is cancelled, logged and given up on, so the process can
    exit anyway; a `stop` that fails is logged and given up on too.

    `stop` runs as a task of its own rather than under asyncio.wait_for():
    with the broker down, aiokafka's consumer.stop() can raise a
    CancelledError of its own making, which wait_for() passes on as if
    this task had been cancelled, ending the process in a traceback. Here
    it's just a failed close, while a real cancellation of the caller
    still propagates. Python 3.10 has no other way to tell the two apart.
    """
    task = asyncio.ensure_future(stop)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        task.cancel()  # this task really was cancelled: take `stop` down with it
        raise
    if not done:
        task.cancel()
        await asyncio.wait({task}, timeout=1)  # let it unwind, but never wait on it for good
        logger.warning("%s didn't %s within %.0fs (broker unreachable?); exiting without it", what, verb, timeout)
        return False
    if task.cancelled() or task.exception() is not None:
        # Cancelled from inside: aiokafka's consumer.stop() leaks the
        # CancelledError of fetch requests it has just cancelled when the
        # broker is unreachable (see backend/kafka_client.py's _stop()).
        reason = "a leaked CancelledError" if task.cancelled() else repr(task.exception())
        logger.warning("%s failed to %s (%s; broker unreachable?); exiting without it", what, verb, reason)
        return False
    return True


async def _main() -> None:
    # Publisher first, so no trade is consumed before an alert could be sent.
    publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC, client_id="dsview-whale-detector")
    await publisher.start()
    try:
        detector = WhaleDetector(on_whale=publisher.publish)
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
        await stop_within(detector.stop(), "Trade consumer")
    finally:
        await stop_within(publisher.stop(), "Alert publisher")
    logger.info("Whale detector stopped")


if __name__ == "__main__":
    asyncio.run(_main())
