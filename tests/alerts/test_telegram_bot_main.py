"""
Tests for `python telegram_bot.py` itself, run as a subprocess the way
scripts/dev.sh runs it: configuration checks, a whale reaching (fake)
Telegram with the real 10 s grouping, and a prompt Ctrl+C.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from aiokafka import AIOKafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, WHALE_ALERTS_TOPIC

from alert_publisher import AlertPublisher
from fake_telegram import CHAT_ID, TOKEN
from telegram_bot import CONSUMER_GROUP_ID
from whale_card import format_card

from helpers import async_wait_for, wait_until_positioned

ALERTS_DIR = Path(__file__).resolve().parents[2] / "alerts"


def _bot_env(**overrides) -> dict:
    env = {name: value for name, value in os.environ.items() if not name.startswith("DSVIEW_TELEGRAM_")}
    env["PYTHONPATH"] = str(ALERTS_DIR.parent)
    env.update(overrides)
    return env


def _run_bot(env: dict, log_path: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "telegram_bot.py"], cwd=ALERTS_DIR, env=env, stdout=log_path.open("w"), stderr=subprocess.STDOUT
    )


def test_refuses_to_start_without_a_token_or_chat(test_services):
    result = subprocess.run(
        [sys.executable, "telegram_bot.py"], cwd=ALERTS_DIR, env=_bot_env(), capture_output=True, text=True, timeout=30
    )
    assert result.returncode != 0
    assert "DSVIEW_TELEGRAM_BOT_TOKEN and DSVIEW_TELEGRAM_CHAT_ID must both be set" in result.stderr


def test_a_wrong_chat_fails_at_startup_with_a_clear_message(fake_telegram, kafka_topics, tmp_path):
    env = _bot_env(
        DSVIEW_TELEGRAM_API_URL=fake_telegram.url, DSVIEW_TELEGRAM_BOT_TOKEN=TOKEN, DSVIEW_TELEGRAM_CHAT_ID="999"
    )
    result = subprocess.run([sys.executable, "telegram_bot.py"], cwd=ALERTS_DIR, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "can't reach chat 999" in result.stderr
    assert TOKEN not in result.stderr + result.stdout


async def _skip_to_the_end_for_the_bots_group() -> None:
    """Commit the topic's end for the bot's real group, so a run starts after earlier tests' alerts."""
    consumer = AIOKafkaConsumer(
        WHALE_ALERTS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=CONSUMER_GROUP_ID, enable_auto_commit=False
    )
    await consumer.start()
    try:
        await wait_until_positioned(consumer)
        await consumer.seek_to_end()
        await consumer.commit({tp: await consumer.position(tp) for tp in consumer.assignment()})
    finally:
        await consumer.stop()


async def test_sends_a_whale_then_stops_promptly_on_ctrl_c(fake_telegram, kafka_topics, symbol, tmp_path):
    await _skip_to_the_end_for_the_bots_group()
    log = tmp_path / "bot.log"
    bot = _run_bot(
        _bot_env(DSVIEW_TELEGRAM_API_URL=fake_telegram.url, DSVIEW_TELEGRAM_BOT_TOKEN=TOKEN, DSVIEW_TELEGRAM_CHAT_ID=CHAT_ID),
        log,
    )
    try:
        await async_wait_for(lambda: "Reading 'alerts.whales'" in log.read_text(), timeout=30, message="the bot never started reading")
        await asyncio.sleep(1)  # let it join the group (it resumes from the commit above either way)

        alert = {
            "event_type": "whale_trade", "exchange": "binance", "symbol": symbol, "trade_id": "1", "side": "buy",
            "price": "97000.10", "quantity": "2.5", "notional": "242500.25", "threshold": "100000",
            "event_time": "2026-11-20T10:16:57Z", "detected_at": "2026-11-20T10:16:57.2+00:00",
        }
        publisher = AlertPublisher(topic=WHALE_ALERTS_TOPIC)
        await publisher.start()
        try:
            await publisher.publish(alert)
        finally:
            await publisher.stop()

        # The real 10 s grouping: sent once 10 s pass with no other whale.
        await async_wait_for(lambda: fake_telegram.messages, timeout=30, message="no message reached Telegram")
        assert fake_telegram.messages == [format_card(alert)]

        bot.send_signal(signal.SIGINT)
        started = time.monotonic()
        assert bot.wait(timeout=15) == 0
        assert time.monotonic() - started < 10
    finally:
        if bot.poll() is None:
            bot.kill()

    output = log.read_text()
    assert "Sent 1 whale to Telegram" in output
    assert "Telegram bot stopped" in output
    assert TOKEN not in output
