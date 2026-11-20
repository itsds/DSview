"""Tests for alerts/telegram_client.py against a fake Bot API: retries, rate limits, configuration errors."""

from __future__ import annotations

import asyncio
import logging
import socket
import time

import pytest

from fake_telegram import CHAT_ID, TOKEN
from telegram_client import TelegramClient, TelegramConfigError, TelegramUnreachable


@pytest.fixture
async def client(fake_telegram):
    telegram = TelegramClient(TOKEN, CHAT_ID, api_url=fake_telegram.url, min_send_interval=0, backoff_initial=0.05)
    await telegram.start()
    try:
        yield telegram
    finally:
        await telegram.close()


async def test_check_names_the_bot(client):
    assert await client.check() == "dsview_test_bot"


async def test_check_rejects_a_bad_token(fake_telegram):
    telegram = TelegramClient("wrong-token", CHAT_ID, api_url=fake_telegram.url)
    await telegram.start()
    try:
        with pytest.raises(TelegramConfigError, match="token"):
            await telegram.check()
    finally:
        await telegram.close()


async def test_check_rejects_a_chat_the_bot_cant_see(fake_telegram):
    telegram = TelegramClient(TOKEN, "999", api_url=fake_telegram.url)
    await telegram.start()
    try:
        with pytest.raises(TelegramConfigError, match="chat not found"):
            await telegram.check()
    finally:
        await telegram.close()


async def test_sends_plain_text(client, fake_telegram):
    assert await client.send_message("🐋 BUY · BTCUSDT") is True
    assert fake_telegram.messages == ["🐋 BUY · BTCUSDT"]


async def test_a_rate_limit_waits_as_long_as_telegram_asks(client, fake_telegram):
    fake_telegram.fail_next(429, "Too Many Requests: retry after 1", retry_after=1)

    started = time.monotonic()
    assert await client.send_message("hello") is True
    assert time.monotonic() - started >= 1.0
    assert fake_telegram.messages == ["hello"]
    assert fake_telegram.send_attempts == 2


async def test_server_errors_are_retried_with_backoff(client, fake_telegram):
    fake_telegram.fail_next(502, "Bad Gateway")
    fake_telegram.fail_next(500, "Internal Server Error")

    assert await client.send_message("hello") is True
    assert fake_telegram.send_attempts == 3


async def test_a_rejected_message_is_dropped_not_retried(client, fake_telegram):
    fake_telegram.fail_next(400, "Bad Request: message is too long")

    assert await client.send_message("x" * 5000) is False
    assert fake_telegram.send_attempts == 1


async def test_a_revoked_token_stops_sending(fake_telegram):
    telegram = TelegramClient("revoked-token", CHAT_ID, api_url=fake_telegram.url, min_send_interval=0)
    await telegram.start()
    try:
        with pytest.raises(TelegramConfigError):
            await telegram.send_message("hello")
    finally:
        await telegram.close()


async def test_sends_are_spaced_out(fake_telegram):
    telegram = TelegramClient(TOKEN, CHAT_ID, api_url=fake_telegram.url, min_send_interval=0.5)
    await telegram.start()
    try:
        started = time.monotonic()
        for text in ("one", "two", "three"):
            await telegram.send_message(text)
        assert time.monotonic() - started >= 1.0  # two gaps of 0.5 s
    finally:
        await telegram.close()


async def test_unreachable_telegram_is_retried_and_the_token_never_logged(caplog):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        dead_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    secret = "123456:SECRET-token-value"
    telegram = TelegramClient(secret, CHAT_ID, api_url=dead_url, min_send_interval=0, backoff_initial=0.01, backoff_max=0.02)
    await telegram.start()
    try:
        with pytest.raises(TelegramUnreachable):
            await telegram.check()

        caplog.set_level(logging.DEBUG)
        send = asyncio.create_task(telegram.send_message("hello"))
        await asyncio.sleep(0.3)  # several failed attempts
        assert not send.done()  # still retrying: an outage never drops a whale
        send.cancel()
    finally:
        await telegram.close()

    assert "Telegram unreachable" in caplog.text
    assert secret not in caplog.text
    assert "SECRET" not in caplog.text
