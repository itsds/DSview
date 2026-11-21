"""
Tests for backfill/binance_rest.py: paging klines and aggTrades from a fake
Binance (tests/fake_binance.py), the rate-limit budget, and what each kind
of failure does. Waits run on a fake clock, so none of this sleeps.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from binance_rest import (
    AGG_TRADE_COLUMNS,
    KLINE_COLUMNS,
    BinanceBanned,
    BinanceHistoryClient,
    BinanceRequestError,
    BinanceUnavailable,
    WeightBudget,
)
from fake_binance import MINUTE_MS

HOUR_MS = 60 * MINUTE_MS
SYMBOL = "BTCUSDT"
# A Monday at midnight UTC, comfortably in the past.
DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)


def client_for(fake, fake_time, **kwargs) -> BinanceHistoryClient:
    budget = kwargs.pop("budget", None) or WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep)
    return BinanceHistoryClient(fake.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock, **kwargs)


async def collect(pages):
    rows = {}
    count = 0
    async for page in pages:
        count += 1
        for column, values in page.items():
            rows.setdefault(column, []).extend(values)
    return count, rows


async def test_klines_page_through_a_range_oldest_first(fake_binance, fake_time):
    start, end = DAY, DAY + 2500 * MINUTE_MS
    async with client_for(fake_binance, fake_time) as client:
        pages, rows = await collect(client.klines(SYMBOL, start, end))

    expected = list(fake_binance.market.klines_between(SYMBOL, start, end))
    assert pages == 3  # 1000 + 1000 + 500
    assert set(rows) == set(KLINE_COLUMNS)
    assert rows["window_start"] == [k[0] for k in expected]
    assert rows["open"] == [k[1] for k in expected]
    assert rows["close"] == [k[4] for k in expected]
    assert rows["volume"] == [k[5] for k in expected]
    assert rows["trade_count"] == [k[8] for k in expected]
    assert rows["taker_buy_volume"] == [k[9] for k in expected]
    assert len(fake_binance.requests_to("/api/v3/klines")) == 3


async def test_klines_skip_an_outage_and_stop_at_the_end(fake_binance, fake_time):
    fake_binance.market.outages = [(DAY + 10 * MINUTE_MS, DAY + 20 * MINUTE_MS)]
    async with client_for(fake_binance, fake_time) as client:
        _, rows = await collect(client.klines(SYMBOL, DAY, DAY + 30 * MINUTE_MS))

    starts = rows["window_start"]
    assert len(starts) == 20
    assert starts[0] == DAY and starts[-1] == DAY + 29 * MINUTE_MS
    assert not any(DAY + 10 * MINUTE_MS <= s < DAY + 20 * MINUTE_MS for s in starts)


async def test_agg_trades_find_the_first_by_time_then_follow_ids(fake_binance, fake_time):
    start, end = DAY + 5 * MINUTE_MS + 30_000, DAY + 600 * MINUTE_MS
    async with client_for(fake_binance, fake_time) as client:
        _, rows = await collect(client.agg_trades(SYMBOL, start, end))

    expected = list(fake_binance.market.trades_between(SYMBOL, start, end))
    assert set(rows) == set(AGG_TRADE_COLUMNS)
    assert rows["agg_trade_id"] == [t["a"] for t in expected]
    assert rows["event_time"] == [t["T"] for t in expected]
    assert rows["price"] == [t["p"] for t in expected]
    assert rows["side"] == ["sell" if t["m"] else "buy" for t in expected]
    assert rows["last_trade_id"] == [t["l"] for t in expected]
    requests = fake_binance.requests_to("/api/v3/aggTrades")
    assert "startTime" in requests[0] and int(requests[0]["endTime"]) - int(requests[0]["startTime"]) < HOUR_MS
    assert all("fromId" in r for r in requests[1:])


async def test_agg_trades_look_past_hours_without_trades(fake_binance, fake_time):
    fake_binance.market.outages = [(DAY, DAY + 150 * MINUTE_MS)]
    async with client_for(fake_binance, fake_time) as client:
        _, rows = await collect(client.agg_trades(SYMBOL, DAY, DAY + 160 * MINUTE_MS))

    assert len(rows["agg_trade_id"]) == 10 * fake_binance.market.trades_per_minute
    assert min(rows["event_time"]) >= DAY + 150 * MINUTE_MS
    by_time = [r for r in fake_binance.requests_to("/api/v3/aggTrades") if "startTime" in r]
    assert len(by_time) == 3  # two empty hours, then the one with trades


async def test_agg_trades_stop_at_the_newest_trade(fake_binance, fake_time):
    fake_binance.now_ms = DAY + 30 * MINUTE_MS
    async with client_for(fake_binance, fake_time) as client:
        _, rows = await collect(client.agg_trades(SYMBOL, DAY, DAY + 60 * MINUTE_MS))
    assert max(rows["event_time"]) < DAY + 30 * MINUTE_MS
    assert len(rows["agg_trade_id"]) == 30 * fake_binance.market.trades_per_minute


async def test_a_429_pauses_for_its_retry_after_and_retries(fake_binance, fake_time):
    fake_binance.fail_next(429, retry_after=7)
    async with client_for(fake_binance, fake_time, max_attempts=1) as client:
        _, rows = await collect(client.klines(SYMBOL, DAY, DAY + 10 * MINUTE_MS))
    assert len(rows["window_start"]) == 10  # a 429 isn't a failed attempt, even with max_attempts=1
    assert 7 in [round(s) for s in fake_time.sleeps]


async def test_a_418_stops_the_run(fake_binance, fake_time):
    fake_binance.fail_next(418, retry_after=120)
    async with client_for(fake_binance, fake_time) as client:
        with pytest.raises(BinanceBanned) as info:
            await collect(client.klines(SYMBOL, DAY, DAY + 10 * MINUTE_MS))
    assert info.value.retry_after == 120
    assert len(fake_binance.requests) == 1


async def test_5xx_retries_with_backoff(fake_binance, fake_time):
    fake_binance.fail_next(500, count=2)
    async with client_for(fake_binance, fake_time) as client:
        _, rows = await collect(client.klines(SYMBOL, DAY, DAY + 10 * MINUTE_MS))
    assert len(rows["window_start"]) == 10
    assert fake_time.sleeps == [1.0, 2.0]


async def test_a_lasting_outage_ends_the_run(fake_binance, fake_time):
    fake_binance.fail_next(503, count=10)
    async with client_for(fake_binance, fake_time, max_attempts=4) as client:
        with pytest.raises(BinanceUnavailable):
            await collect(client.klines(SYMBOL, DAY, DAY + 10 * MINUTE_MS))
    assert len(fake_binance.requests) == 4


async def test_an_unknown_symbol_fails_at_once_with_binances_message(fake_binance, fake_time):
    async with client_for(fake_binance, fake_time) as client:
        with pytest.raises(BinanceRequestError, match="Invalid symbol"):
            await collect(client.klines("NOPE", DAY, DAY + 10 * MINUTE_MS))
    assert len(fake_binance.requests) == 1


async def test_network_errors_retry(fake_time):
    # Nothing listens on port 9: every attempt is a connection error.
    client = BinanceHistoryClient(
        "http://127.0.0.1:9",
        budget=WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep),
        sleep=fake_time.sleep,
        clock=fake_time.clock,
        max_attempts=3,
    )
    async with client:
        with pytest.raises(BinanceUnavailable, match="ConnectError"):
            await collect(client.klines(SYMBOL, DAY, DAY + MINUTE_MS))
    assert fake_time.sleeps == [1.0, 2.0]


async def test_the_budget_caps_weight_per_wall_clock_minute(fake_time):
    budget = WeightBudget(per_minute=10, wall_clock=fake_time.clock, sleep=fake_time.sleep)
    fake_time.now = 6_000.0  # the start of a minute
    for _ in range(5):
        await budget.acquire(2)
    assert fake_time.now == 6_000.0  # 10 weight fits in the minute
    await budget.acquire(2)
    assert fake_time.now == 6_060.0  # the sixth waited for the next minute


async def test_the_budget_takes_binances_count_into_account(fake_time):
    budget = WeightBudget(per_minute=100, wall_clock=fake_time.clock, sleep=fake_time.sleep)
    fake_time.now = 6_030.0
    await budget.acquire(4)
    budget.observe("99")  # something else on this IP used most of the minute
    await budget.acquire(4)
    assert fake_time.now == 6_060.0


async def test_a_pause_holds_every_request(fake_time):
    budget = WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep)
    budget.pause(30)
    await budget.acquire(2)
    assert fake_time.sleeps == [30]
