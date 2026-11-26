"""
telegram_bot.py

Sends whale alerts to Telegram: the last leg of the alerts lane,
streaming/jobs/whale_detector.py -> alerts.whales -> here -> your phone.
Every whale the right panel shows is sent; whales within 10 seconds of
each other share one message (whale_batcher.py), each one a three-line
card (whale_card.py). Telegram itself is behind telegram_client.py.

Reads alerts.whales in its own consumer group, dsview-telegram-bot, so it
gets every alert whatever the backend's /ws/whales sockets read, and
resumes where it left off after a restart. On its very first start it
begins at the end of the topic rather than sending the whole history.

Delivery is at-least-once. Offsets are committed by hand, and only past
alerts that have been sent: a whale still waiting for its group to close,
or for Telegram to come back, keeps its offset uncommitted, so a crash or
restart reads it again rather than losing it. Repeats are ignored on
(exchange, trade_id) — the detector can publish the same whale twice
(see its docstring), and a re-read after a restart would repeat the
last few — for the last SEEN_KEYS_KEPT whales. A whale sent just before
a crash and re-read after it is the one repeat that can still reach the
phone.

On Ctrl+C or SIGTERM the open group is sent at once rather than held for
its 10 seconds; like the detector, each closing step gets
SHUTDOWN_TIMEOUT_SECONDS, so a dead broker or Telegram can't stop the
bot from exiting (anything unsent is simply read again next start).

The token and chat id come from DSVIEW_TELEGRAM_BOT_TOKEN and
DSVIEW_TELEGRAM_CHAT_ID (config/settings.py), never from a file in git;
scripts/dev.sh loads them from the repo root's gitignored .env. Run from
alerts/, with the repo root on PYTHONPATH:

    cd alerts && python telegram_bot.py

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from collections import OrderedDict, deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    LIVE_PIPELINE,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)

from telegram_client import TelegramClient, TelegramConfigError, TelegramUnreachable
from whale_batcher import WhaleBatcher
from whale_card import format_message, format_usd, parse_time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP_ID = "dsview-telegram-bot"
# Live's whales, whatever DSVIEW_PIPELINE says: a replayed whale never reaches
# the phone (Phase 4a's kickoff decision 1).
WHALE_ALERTS_TOPIC = LIVE_PIPELINE.whale_alerts_topic
SEEN_KEYS_KEPT = 10_000
SHUTDOWN_TIMEOUT_SECONDS = 5.0
# Longest a poll waits with no group open; a new alert ends it at once.
IDLE_POLL_SECONDS = 5.0

Alert = Dict[str, Any]
Item = Tuple[TopicPartition, int, Alert]  # where the alert sits on the topic, and the alert


class WhaleTelegramBot:
    """Consumes alerts.whales and sends each whale, grouped, through a TelegramClient."""

    def __init__(
        self,
        telegram: TelegramClient,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = WHALE_ALERTS_TOPIC,
        group_id: str = CONSUMER_GROUP_ID,
        batcher: Optional[WhaleBatcher] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._telegram = telegram
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._batcher: WhaleBatcher[Item] = batcher or WhaleBatcher()
        self._clock = clock
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._ready: Deque[List[Item]] = deque()  # closed groups, oldest first, not yet sent
        self._seen: "OrderedDict[Tuple[str, str], None]" = OrderedDict()
        self._consumed: Dict[TopicPartition, int] = {}  # next offset to read, per partition
        self._committed: Dict[TopicPartition, int] = {}

    async def start(self) -> None:
        """Connect and join the consumer group. Call once before run()."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,  # committed only once sent: see the module docstring
            auto_offset_reset="latest",
            value_deserializer=lambda v: v,
        )
        await self._consumer.start()
        logger.info("Reading '%s' from %s (group=%s)", self._topic, self._bootstrap_servers, self._group_id)

    async def run(self) -> None:
        """Consume and send until cancelled. Raises TelegramConfigError if the token stops working."""
        if self._consumer is None:
            raise RuntimeError("WhaleTelegramBot.start() must be called before run()")
        while True:
            batch = await self._consumer.getmany(timeout_ms=int(self._poll_timeout() * 1000))
            for tp, records in batch.items():
                for record in records:
                    self._consumed[tp] = record.offset + 1
                    self._accept(tp, record.offset, record.value)
            due = self._batcher.pop_due(self._clock())
            if due:
                self._ready.append(due)
            await self._send_ready()
            await self._commit()

    async def flush(self) -> None:
        """Send everything held, the open group included, and commit. For shutdown."""
        group = self._batcher.pop()
        if group:
            self._ready.append(group)
        await self._send_ready()
        await self._commit()

    async def stop(self) -> None:
        """Leave the group and close the consumer. Call once at shutdown, after flush()."""
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("Alert consumer stopped")

    def _poll_timeout(self) -> float:
        if self._ready:
            return 0.0
        due_in = self._batcher.due_in(self._clock())
        return IDLE_POLL_SECONDS if due_in is None else min(due_in, IDLE_POLL_SECONDS)

    def _accept(self, tp: TopicPartition, offset: int, raw: bytes) -> None:
        """Hand one alert to the batcher, unless it's malformed or a repeat."""
        try:
            alert = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.exception("Skipping an alert that isn't JSON (%s offset %d)", tp, offset)
            return
        if not isinstance(alert, dict):
            logger.error("Skipping an alert that isn't a JSON object (%s offset %d)", tp, offset)
            return

        trade_id = alert.get("trade_id")
        if trade_id is not None:  # without one there's no telling repeats apart
            key = (str(alert.get("exchange")), str(trade_id))
            if key in self._seen:
                logger.info("Skipping a repeat of whale %s:%s", *key)
                return
            self._seen[key] = None
            if len(self._seen) > SEEN_KEYS_KEPT:
                self._seen.popitem(last=False)

        traded = parse_time(alert.get("event_time")) or parse_time(alert.get("detected_at"))
        closed = self._batcher.add((tp, offset, alert), traded.timestamp() if traded else None, self._clock())
        self._ready.extend(closed)

    async def _send_ready(self) -> None:
        # Sent before it's removed, so a cancellation mid-send leaves the
        # group queued (and its offsets uncommitted) rather than lost.
        while self._ready:
            group = self._ready[0]
            alerts = [alert for _, _, alert in group]
            if await self._telegram.send_message(format_message(alerts)):
                logger.info("Sent %s to Telegram: %s", _count(alerts), ", ".join(_summary(a) for a in alerts))
            self._ready.popleft()

    async def _commit(self) -> None:
        """Commit each partition up to its first whale not yet sent (or all it has read)."""
        held = [item for group in self._ready for item in group] + self._batcher.pending
        offsets = {}
        for tp, next_offset in self._consumed.items():
            first_held = min((offset for held_tp, offset, _ in held if held_tp == tp), default=next_offset)
            offset = min(next_offset, first_held)
            if self._committed.get(tp) != offset:
                offsets[tp] = offset
        if not offsets:
            return
        try:
            await self._consumer.commit(offsets)
            self._committed.update(offsets)
        except KafkaError as exc:
            # Not fatal: the next commit covers these offsets too.
            logger.warning("Couldn't commit offsets yet (%r)", exc)


def _count(alerts: List[Alert]) -> str:
    return "1 whale" if len(alerts) == 1 else f"{len(alerts)} whales"


def _summary(alert: Alert) -> str:
    return f"{alert.get('symbol')} {alert.get('side')} {format_usd(alert.get('notional'))} (trade {alert.get('trade_id')})"


async def stop_within(
    stop: Awaitable[None], what: str, timeout: float = SHUTDOWN_TIMEOUT_SECONDS, verb: str = "close"
) -> bool:
    """
    Await `stop` for at most `timeout` seconds; True if it finished cleanly.

    The same as streaming/jobs/whale_detector.py's (its docstring has the
    detail): with the broker down, aiokafka's consumer.stop() can retry its
    last commit forever or raise a CancelledError of its own making, and a
    send waits for Telegram as long as it takes. Either way the bot logs
    it and exits rather than hanging or ending in a traceback.
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
    telegram = TelegramClient(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    await telegram.start()
    try:
        try:
            username = await telegram.check()
            logger.info("Sending whale alerts as @%s to chat %s", username, TELEGRAM_CHAT_ID)
        except TelegramUnreachable:
            # Not a configuration problem: sends retry until Telegram answers.
            logger.warning("Telegram unreachable; starting anyway, the token is checked on the first send")

        bot = WhaleTelegramBot(telegram)
        await bot.start()

        stop_event = asyncio.Event()

        def _handle_shutdown_signal() -> None:
            logger.info("Shutdown signal received")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_shutdown_signal)

        run_task = asyncio.create_task(bot.run())
        stop_task = asyncio.create_task(stop_event.wait())
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

        logger.info("Stopping Telegram bot...")
        stop_task.cancel()
        run_task.cancel()
        await asyncio.gather(run_task, stop_task, return_exceptions=True)
        failure = None if run_task.cancelled() else run_task.exception()

        if failure is None:
            await stop_within(bot.flush(), "The whales still held", verb="send")
        await stop_within(bot.stop(), "Alert consumer")
        if failure is not None:
            raise failure
    finally:
        await telegram.close()
    logger.info("Telegram bot stopped")


def main() -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        sys.exit(
            "DSVIEW_TELEGRAM_BOT_TOKEN and DSVIEW_TELEGRAM_CHAT_ID must both be set: put them in the "
            "repo root's .env (copy .env.example), which scripts/dev.sh loads, or export them."
        )
    try:
        asyncio.run(_main())
    except TelegramConfigError as exc:
        sys.exit(f"Telegram bot stopped: {exc}")


if __name__ == "__main__":
    main()
