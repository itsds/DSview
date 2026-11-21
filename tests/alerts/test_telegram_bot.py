"""
Tests for alerts/telegram_bot.py: alerts on a real alerts.whales topic,
messages to a fake Telegram. Grouping uses a 1 s gap instead of 10 s to
keep the tests quick; whale_batcher's own tests cover the real numbers.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from aiokafka import AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, WHALE_ALERTS_TOPIC

from alert_publisher import AlertPublisher
from fake_telegram import CHAT_ID, TOKEN
from telegram_bot import WhaleTelegramBot
from telegram_client import TelegramClient
from whale_batcher import WhaleBatcher
from whale_card import format_card

from helpers import async_wait_for, wait_until_positioned

T0 = datetime(2026, 11, 20, 10, 16, 57, tzinfo=timezone.utc)


def _alert(symbol: str, trade_id: str, seconds: float = 0, side: str = "buy") -> dict:
    return {
        "event_type": "whale_trade",
        "exchange": "binance",
        "symbol": symbol,
        "trade_id": trade_id,
        "side": side,
        "price": "97000.10",
        "quantity": "2.5",
        "notional": "242500.25",
        "threshold": "100000",
        "event_time": (T0 + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z"),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
async def publish(kafka_topics):
    publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC)
    await publisher.start()
    try:
        yield publisher.publish
    finally:
        await publisher.stop()


@pytest.fixture
def group_id() -> str:
    return f"test-telegram-bot-{uuid.uuid4().hex}"


class RunningBot:
    """A WhaleTelegramBot running in the background, the way _main() runs it."""

    def __init__(self, fake_telegram, group_id: str, gap: float) -> None:
        self.telegram = TelegramClient(TOKEN, CHAT_ID, api_url=fake_telegram.url, min_send_interval=0)
        self.bot = WhaleTelegramBot(self.telegram, group_id=group_id, batcher=WhaleBatcher(gap_seconds=gap))
        self.task = None

    async def start(self) -> "RunningBot":
        await self.telegram.start()
        await self.bot.start()
        await wait_until_positioned(self.bot._consumer)
        self.task = asyncio.create_task(self.bot.run())
        return self

    async def stop(self, flush: bool = True) -> None:
        """Ctrl+C (flush, then leave), or with flush=False a crash: nothing more is sent or committed."""
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        if flush:
            await self.bot.flush()
        await self.bot.stop()
        await self.telegram.close()


@pytest.fixture
async def start_bot(fake_telegram, group_id):
    bots = []

    async def start(gap: float = 1.0, group: str = group_id) -> RunningBot:
        bot = await RunningBot(fake_telegram, group, gap).start()
        bots.append(bot)
        return bot

    yield start
    for bot in bots:
        if not bot.task.done():
            await bot.stop()


async def _messages(fake_telegram, count: int, timeout: float = 15):
    await async_wait_for(lambda: len(fake_telegram.messages) >= count, timeout=timeout,
                         message=f"expected {count} Telegram message(s), got {fake_telegram.messages}")
    return fake_telegram.messages


async def test_close_whales_share_one_message_and_repeats_are_ignored(start_bot, publish, fake_telegram, symbol):
    await start_bot()
    first, second = _alert(symbol, "1"), _alert(symbol, "2", seconds=0.4, side="sell")
    await publish(first)
    await publish(first)  # the detector repeating itself
    await publish(second)

    [message] = await _messages(fake_telegram, 1)
    assert message == format_card(first) + "\n\n" + format_card(second)
    await asyncio.sleep(1.5)
    assert len(fake_telegram.messages) == 1  # the repeat didn't become a second message


async def test_whales_far_apart_are_separate_messages(start_bot, publish, fake_telegram, symbol):
    await start_bot()
    await publish(_alert(symbol, "1"))
    await publish(_alert(symbol, "2", seconds=30))

    messages = await _messages(fake_telegram, 2)
    assert messages == [format_card(_alert(symbol, "1")), format_card(_alert(symbol, "2", seconds=30))]


async def test_a_restart_resumes_after_what_was_sent(start_bot, publish, fake_telegram, symbol):
    bot = await start_bot()
    await publish(_alert(symbol, "before"))
    await _messages(fake_telegram, 1)
    # The fake records a message as its request arrives, before replying.
    # Stopping while the bot still awaits that reply would rightly send it
    # again on flush (at-least-once: it can't know it got through), so let
    # the send finish and be committed first. Flaky in CI before this.
    await async_wait_for(lambda: bot.bot._committed and not bot.bot._ready, timeout=10)
    await bot.stop()

    await publish(_alert(symbol, "while-stopped", side="sell"))  # a card that reads differently
    await start_bot()  # same consumer group

    messages = await _messages(fake_telegram, 2)
    assert messages[1] == format_card(_alert(symbol, "while-stopped", side="sell"))
    await asyncio.sleep(1.5)
    assert len(fake_telegram.messages) == 2  # "before" wasn't sent again


async def test_ctrl_c_sends_the_open_group_at_once(start_bot, publish, fake_telegram, symbol):
    bot = await start_bot(gap=60)  # would otherwise hold the whale for a minute
    await publish(_alert(symbol, "1"))
    await async_wait_for(lambda: bot.bot._batcher.pending, timeout=10)
    assert fake_telegram.messages == []

    await bot.stop()
    assert fake_telegram.messages == [format_card(_alert(symbol, "1"))]


async def test_a_whale_held_at_a_crash_is_read_again(start_bot, publish, fake_telegram, symbol):
    bot = await start_bot(gap=60)
    await publish(_alert(symbol, "held"))
    await async_wait_for(lambda: bot.bot._batcher.pending, timeout=10)
    await bot.stop(flush=False)  # crash: nothing sent, offset not committed past it

    await start_bot()  # same group, picks up from the last commit
    [message] = await _messages(fake_telegram, 1)
    assert message == format_card(_alert(symbol, "held"))


async def test_malformed_alerts_are_skipped(start_bot, publish, fake_telegram, symbol):
    await start_bot()
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(WHALE_ALERTS_TOPIC, b"not json", key=symbol.encode())
        await producer.send_and_wait(WHALE_ALERTS_TOPIC, b"[1, 2]", key=symbol.encode())
    finally:
        await producer.stop()
    await publish(_alert(symbol, "fine"))

    [message] = await _messages(fake_telegram, 1)
    assert message == format_card(_alert(symbol, "fine"))
