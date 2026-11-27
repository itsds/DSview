"""
Tests for ingestion/futures_poller.py against a fake Binance
(tests/fake_binance.py) on a fake clock: the first start's 30-day catch-up
(the backfill), resuming after the last period published, live periods as
Binance publishes them, holes, outages, and the 30-day limit. Each run ends
when the fake clock passes a horizon.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from binance_futures_rest import PERIOD_MS, BinanceFuturesClient, BinanceRequestError, RequestBudget
from config.settings import FUTURES_RATIO_KINDS
from futures_poller import FuturesRatioPoller

SYMBOL = "BTCUSDT"
DAY_MS = 86_400_000
MINUTE = 60.0


class Stop(Exception):
    """The test's horizon: raised from the poller's sleep once the fake clock passes it."""


@pytest.fixture
def fake(fake_binance, fake_time):
    fake_binance.clock = fake_time.clock
    return fake_binance


def now_ms(fake_time) -> int:
    return int(fake_time.clock() * 1000)


def latest_closed(fake_time) -> int:
    return now_ms(fake_time) // PERIOD_MS * PERIOD_MS


async def poll(fake, fake_time, run_for: float, **kwargs):
    """Run a poller for `run_for` fake seconds; (poller, every page it handed on)."""
    pages = []
    horizon = fake_time.clock() + run_for

    async def sleep(seconds):
        await fake_time.sleep(seconds)
        if fake_time.clock() >= horizon:
            # Every follower moves the one fake clock: let any still fetching finish first.
            await asyncio.sleep(0.3)
            raise Stop

    async def on_ratios(page):
        pages.append(page)

    budget = RequestBudget(clock=fake_time.clock, sleep=fake_time.sleep)
    kwargs.setdefault("kinds", ["taker_volume"])
    async with BinanceFuturesClient(fake.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock) as client:
        poller = FuturesRatioPoller(
            client, kwargs.pop("symbols", [SYMBOL]), on_ratios, clock=fake_time.clock, sleep=sleep, **kwargs
        )
        with pytest.raises(Stop):
            await poller.run()
    return poller, pages


def starts(pages, symbol=SYMBOL, kind="taker_volume"):
    return [int(e.event_time.timestamp() * 1000) for page in pages for e in page if (e.symbol, e.kind) == (symbol, kind)]


async def test_a_first_start_fetches_the_30_days_binance_keeps_then_follows_live(fake, fake_time):
    first_closed = latest_closed(fake_time)
    _, pages = await poll(fake, fake_time, run_for=20 * MINUTE)

    got = starts(pages)
    assert got == sorted(set(got)), "every period once, oldest first"
    assert got[0] >= now_ms(fake_time) - 30 * DAY_MS - 20 * 60_000  # 30 days before the run began
    assert got[0] - first_closed < -29 * DAY_MS
    assert got == list(range(got[0], got[-1] + PERIOD_MS, PERIOD_MS)), "no gaps"
    # Live: every period that closed during the run, apart from one closing in the last few seconds.
    assert got[-1] >= latest_closed(fake_time) - 2 * PERIOD_MS
    assert len(pages[0]) == 500  # caught up a page at a time


async def test_a_restart_resumes_after_the_newest_period_published(fake, fake_time):
    closed = latest_closed(fake_time)
    last = closed - 12 * PERIOD_MS  # off for an hour
    _, pages = await poll(fake, fake_time, run_for=MINUTE, resume_from={(SYMBOL, "taker_volume"): last})
    assert starts(pages) == list(range(last + PERIOD_MS, closed, PERIOD_MS))


async def test_a_gap_longer_than_binance_keeps_loses_the_oldest_periods(fake, fake_time, caplog):
    last = now_ms(fake_time) - 45 * DAY_MS
    with caplog.at_level(logging.WARNING, logger="futures_poller"):
        _, pages = await poll(fake, fake_time, run_for=MINUTE, resume_from={(SYMBOL, "taker_volume"): last})
    assert starts(pages)[0] >= now_ms(fake_time) - MINUTE * 1000 - 30 * DAY_MS
    assert "older than Binance keeps, and lost" in caplog.text


async def test_refetch_days_fetches_again_whatever_was_published(fake, fake_time):
    newest = latest_closed(fake_time) - PERIOD_MS
    _, pages = await poll(fake, fake_time, run_for=MINUTE, resume_from={(SYMBOL, "taker_volume"): newest}, refetch_days=1)
    got = starts(pages)
    assert len(got) >= 287 and got[-1] == newest


async def test_a_period_is_asked_for_again_until_binance_publishes_it(fake, fake_time):
    fake.publish_delay_ms = 50_000  # Binance is slow today
    newest = latest_closed(fake_time) - PERIOD_MS
    poller, pages = await poll(
        fake, fake_time, run_for=12 * MINUTE, resume_from={(SYMBOL, "taker_volume"): newest}, publish_delay=5, retry=15
    )
    got = starts(pages)
    assert got == list(range(newest + PERIOD_MS, got[-1] + PERIOD_MS, PERIOD_MS)) and len(got) >= 2
    # Asked 5 s after each close, then every 15 s: four tries before a 50 s delay is over.
    retries = [s for s in fake_time.sleeps if s == 15]
    assert len(retries) >= 3 * len(got)


async def test_a_hole_binance_never_fills_is_passed_once_the_next_period_is_out(fake, fake_time):
    newest = latest_closed(fake_time) - PERIOD_MS
    hole = newest + PERIOD_MS
    fake.ratios.holes = [("taker_volume", hole)]
    _, pages = await poll(fake, fake_time, run_for=25 * MINUTE, resume_from={(SYMBOL, "taker_volume"): newest})
    got = starts(pages)
    assert hole not in got and got[0] == hole + PERIOD_MS
    assert got == list(range(hole + PERIOD_MS, got[-1] + PERIOD_MS, PERIOD_MS))


async def test_binance_being_down_pauses_a_ratio_and_loses_nothing(fake, fake_time, caplog):
    newest = latest_closed(fake_time) - 3 * PERIOD_MS
    fake.fail_next(503, count=8)  # the client's every attempt: BinanceUnavailable
    with caplog.at_level(logging.WARNING, logger="futures_poller"):
        _, pages = await poll(fake, fake_time, run_for=5 * MINUTE, resume_from={(SYMBOL, "taker_volume"): newest})
    assert "trying again in 60 s" in caplog.text
    got = starts(pages)
    assert got[:2] == [newest + PERIOD_MS, newest + 2 * PERIOD_MS]


async def test_a_ban_is_waited_out(fake, fake_time, caplog):
    newest = latest_closed(fake_time) - 2 * PERIOD_MS
    fake.fail_next(418, retry_after=300)
    with caplog.at_level(logging.ERROR, logger="futures_poller"):
        _, pages = await poll(fake, fake_time, run_for=10 * MINUTE, resume_from={(SYMBOL, "taker_volume"): newest})
    assert "waiting 300 s" in caplog.text
    assert starts(pages)[0] == newest + PERIOD_MS


async def test_a_request_binance_refuses_ends_the_run(fake, fake_time):
    async with BinanceFuturesClient(fake.url, sleep=fake_time.sleep, clock=fake_time.clock) as client:
        poller = FuturesRatioPoller(
            client, ["BTCBUSD"], lambda page: None, clock=fake_time.clock, sleep=fake_time.sleep, history_days=1
        )
        with pytest.raises(BinanceRequestError, match="Invalid symbol"):
            await poller.run()


async def test_every_symbol_and_ratio_is_followed_on_its_own(fake, fake_time):
    symbols = [SYMBOL, "ETHUSDT"]
    poller, pages = await poll(
        fake, fake_time, run_for=15 * MINUTE, symbols=symbols, kinds=list(FUTURES_RATIO_KINDS), history_days=0.5
    )
    for symbol in symbols:
        for kind in FUTURES_RATIO_KINDS:
            got = starts(pages, symbol, kind)
            assert len(got) >= 144, (symbol, kind)
            assert got == list(range(got[0], got[-1] + PERIOD_MS, PERIOD_MS)), (symbol, kind)
            assert poller.published[(symbol, kind)] == got[-1]


async def test_a_period_still_missing_ten_minutes_after_it_closed_is_logged_once(fake, fake_time, caplog):
    fake.publish_delay_ms = 11 * 60_000
    newest = latest_closed(fake_time) - PERIOD_MS
    with caplog.at_level(logging.WARNING, logger="futures_poller"):
        _, pages = await poll(fake, fake_time, run_for=20 * MINUTE, resume_from={(SYMBOL, "taker_volume"): newest})
    assert caplog.text.count("hasn't published") == 1
    assert starts(pages)[0] == newest + PERIOD_MS  # it came in the end
