"""Tests for alerts/telegram_bot.py's stop_within(), which keeps a dead broker or Telegram from blocking exit."""

from __future__ import annotations

import asyncio
import time

from telegram_bot import stop_within


async def test_stop_within_gives_up_on_a_stop_that_never_returns():
    async def never_returns() -> None:
        await asyncio.Event().wait()

    started = time.monotonic()
    assert await stop_within(never_returns(), "Stuck client", timeout=0.2) is False
    assert time.monotonic() - started < 2


async def test_stop_within_lets_a_prompt_stop_finish():
    async def prompt() -> None:
        await asyncio.sleep(0.01)

    assert await stop_within(prompt(), "Client", timeout=5) is True


async def test_stop_within_survives_a_cancelled_error_leaked_from_inside_stop(caplog):
    # What aiokafka's consumer.stop() does with the broker down: its fetcher
    # re-raises the CancelledError of requests it has just cancelled.
    async def leaks_cancellation() -> None:
        raise asyncio.CancelledError()

    assert await stop_within(leaks_cancellation(), "Leaky client", timeout=5) is False
    assert "leaked CancelledError" in caplog.text


async def test_stop_within_survives_a_stop_that_raises(caplog):
    async def fails() -> None:
        raise ConnectionError("broker gone")

    assert await stop_within(fails(), "Failing client", timeout=5) is False
    assert "broker gone" in caplog.text


async def test_a_real_cancellation_of_the_caller_still_propagates():
    stop_cancelled = asyncio.Event()

    async def slow_stop() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stop_cancelled.set()
            raise

    caller = asyncio.create_task(stop_within(slow_stop(), "Client", timeout=30))
    await asyncio.sleep(0.05)
    caller.cancel()
    try:
        await caller
        raise AssertionError("the caller's cancellation was swallowed")
    except asyncio.CancelledError:
        pass
    await asyncio.wait_for(stop_cancelled.wait(), 1)  # and the stop was cancelled with it
