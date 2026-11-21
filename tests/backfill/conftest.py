"""Fixtures for the backfill's tests."""

from __future__ import annotations

import pytest

from fake_binance import FakeBinance


class FakeTime:
    """A wall clock that only moves when something sleeps on it: rate-limit and backoff waits cost no real time."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start
        self.sleeps = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += max(seconds, 0)


@pytest.fixture
def fake_binance():
    """A fake Binance REST API on a free local port, over fake_binance.SyntheticMarket."""
    fake = FakeBinance().start()
    try:
        yield fake
    finally:
        fake.stop()


@pytest.fixture
def fake_time() -> FakeTime:
    return FakeTime()
