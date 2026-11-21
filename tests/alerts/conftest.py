"""Fixtures for the Telegram bot's tests."""

from __future__ import annotations

import pytest

from fake_telegram import FakeTelegram


@pytest.fixture
def fake_telegram():
    """A fake Bot API on a free local port; the bot's token and chat are fake_telegram.TOKEN and .CHAT_ID."""
    fake = FakeTelegram().start()
    try:
        yield fake
    finally:
        fake.stop()
