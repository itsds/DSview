"""
futures_ratios.py

The long/short ratios' consumer (Phase 4c): every 5-minute period
ingestion/futures_main.py publishes to `market.futures.ratios` goes into
Postgres's futures_ratios table (FuturesRatioPostgresSink), and the news of
what changed out on Redis (FuturesRatioRedisSink) for the backend to push.

A plain asyncio consumer, like the whale detector: storing a period needs
no windowing or state across events, so no Spark. Reads the raw JSON
rather than importing ingestion/schema.py's FuturesRatio, for the same
reason the detector gives: once on the wire, an event is just JSON.

- **From the topic's start, the first time** (`earliest`, unlike the trade
  consumers' `latest`): the poller's first start publishes the 30 days
  Binance keeps, maybe before this consumer has ever run, and every
  period is history worth keeping. After that, its group's committed
  offsets.
- **At-least-once**: offsets are committed by hand, only once Postgres has
  the batch. A crash re-reads the uncommitted batch; the upsert rewrites a
  row only when a value differs, so repeats change nothing (and push
  nothing). While Postgres is down, a batch is retried with backoff (1 s
  doubling to 30 s) from a fresh connection, and nothing is committed.
- **The push is best-effort**: Postgres holds the truth and charts load it
  over REST, so a Redis error is logged and the batch still committed.

Live only: the replay has no Futures feed, and the table is live's alone.

    cd streaming && python -m jobs.futures_ratios

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional

import psycopg
import redis
from aiokafka import AIOKafkaConsumer

from config.settings import FUTURES_RATIO_KINDS, FUTURES_RATIOS_TOPIC, KAFKA_BOOTSTRAP_SERVERS, PIPELINE

from jobs.whale_detector import stop_within
from postgres_sink import FuturesRatioPostgresSink
from redis_sink import FuturesRatioRedisSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP_ID = PIPELINE.consumer_group("dsview-futures-ratios")
BATCH_MAX_RECORDS = 5_000  # a poller's catch-up page is 500 periods
POLL_TIMEOUT_MS = 1_000
RETRY_INITIAL_SECONDS = 1.0
RETRY_MAX_SECONDS = 30.0

Row = Dict[str, Any]


def parse_ratio(raw_value: bytes) -> Optional[Row]:
    """A futures_ratio message as a futures_ratios row, or None (logged) if it isn't one."""
    try:
        message = json.loads(raw_value)
        if message.get("event_type") != "futures_ratio":
            return None
        start = datetime.fromisoformat(message["event_time"].replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        row = {
            "symbol": message["symbol"],
            "period_start": start.astimezone(timezone.utc),
            "kind": message["kind"],
            "ratio": Decimal(str(message["ratio"])),
            "long_value": Decimal(str(message["long_value"])),
            "short_value": Decimal(str(message["short_value"])),
        }
    except (ValueError, KeyError, TypeError, AttributeError, InvalidOperation):
        logger.warning("Skipping a malformed ratio message: %r", raw_value[:300])
        return None
    if row["kind"] not in FUTURES_RATIO_KINDS or not all(row[f].is_finite() for f in ("ratio", "long_value", "short_value")):
        logger.warning("Skipping a ratio message with an unknown ratio or a non-finite value: %r", raw_value[:300])
        return None
    return row


class FuturesRatioConsumer:
    """Consumes market.futures.ratios into Postgres, publishing what changed to Redis."""

    def __init__(
        self,
        postgres: Callable[[], FuturesRatioPostgresSink] = FuturesRatioPostgresSink,
        redis_sink: Optional[FuturesRatioRedisSink] = None,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = FUTURES_RATIOS_TOPIC,
        group_id: str = CONSUMER_GROUP_ID,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._connect_postgres = postgres
        self._postgres: Optional[FuturesRatioPostgresSink] = None
        self._redis = redis_sink or FuturesRatioRedisSink()
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._sleep = sleep
        self._consumer: Optional[AIOKafkaConsumer] = None
        self.stored = 0  # rows written that were new or changed, for the logs and the tests

    async def start(self) -> None:
        """Connect and subscribe. Call once before run()."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,  # committed after Postgres has each batch
            auto_offset_reset="earliest",  # the first time: the whole topic, backfill included
            value_deserializer=lambda v: v,
        )
        await self._consumer.start()
        logger.info("Consuming '%s' from %s (group=%s)", self._topic, self._bootstrap_servers, self._group_id)

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
        if self._postgres is not None:
            self._postgres.close()
            self._postgres = None

    async def run(self) -> None:
        """Consume until cancelled."""
        if self._consumer is None:
            raise RuntimeError("FuturesRatioConsumer.start() must be called before run()")
        while True:
            batch = await self._consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_MAX_RECORDS)
            if not batch:
                continue
            rows = [row for records in batch.values() for record in records if (row := parse_ratio(record.value))]
            if rows:
                changed = await self._store(rows)
                self.stored += len(changed)
                if len(rows) > 1 or changed:
                    logger.info("Stored %d ratio periods (%d new or changed)", len(rows), len(changed))
                self._publish(changed)
            await self._consumer.commit()

    async def _store(self, rows: List[Row]) -> list:
        """Upsert `rows`, retrying from a fresh connection while Postgres is unreachable."""
        delay = RETRY_INITIAL_SECONDS
        while True:
            try:
                if self._postgres is None:
                    self._postgres = await asyncio.to_thread(self._connect_postgres)
                return await asyncio.to_thread(self._postgres.write_rows, rows)
            except psycopg.errors.UndefinedTable as exc:
                raise RuntimeError("Postgres has no futures_ratios table: run `python storage/init_db.py`") from exc
            except psycopg.OperationalError as exc:
                logger.warning("Postgres unavailable (%s); retrying the batch in %.0f s", exc, delay)
                if self._postgres is not None:
                    try:
                        self._postgres.close()
                    except psycopg.Error:
                        pass
                    self._postgres = None
                await self._sleep(delay)
                delay = min(delay * 2, RETRY_MAX_SECONDS)

    def _publish(self, changed: list) -> None:
        try:
            self._redis.publish_changes(changed)
        except redis.RedisError as exc:
            logger.warning("Couldn't publish ratio changes to Redis (%s); charts pick them up on reload", exc)


async def _main() -> None:
    if not PIPELINE.is_live:
        raise SystemExit(f"the ratios consumer is the live pipeline's only (DSVIEW_PIPELINE is {PIPELINE.name!r})")
    consumer = FuturesRatioConsumer()
    await consumer.start()

    stop_event = asyncio.Event()

    def _handle_shutdown_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    run_task = asyncio.create_task(consumer.run())
    stopping = asyncio.create_task(stop_event.wait())
    await asyncio.wait({run_task, stopping}, return_when=asyncio.FIRST_COMPLETED)
    logger.info("Stopping the ratios consumer...")
    for task in (run_task, stopping):
        task.cancel()
    results = await asyncio.gather(run_task, stopping, return_exceptions=True)
    await stop_within(consumer.stop(), "Ratio consumer")
    if isinstance(results[0], Exception):
        logger.error("Ratios consumer failed: %r", results[0])
        raise SystemExit(1)
    logger.info("Ratios consumer stopped")


if __name__ == "__main__":
    asyncio.run(_main())
