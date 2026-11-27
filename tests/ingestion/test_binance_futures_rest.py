"""
Tests for ingestion/binance_futures_rest.py: paging Binance Futures' four
long/short ratios from a fake Binance (tests/fake_binance.py), the
perpetual-only parameter shapes and the check that settles them, the
request budget, and what each kind of failure does. Waits run on a fake
clock, so none of this sleeps.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from binance_futures_rest import (
    PAGE_LIMIT,
    PATHS,
    PERIOD_MS,
    BinanceBanned,
    BinanceFuturesClient,
    BinanceRequestError,
    BinanceUnavailable,
    ParamShape,
    RequestBudget,
)
from config.settings import FUTURES_RATIO_KINDS
from fake_binance import LEGACY_FUTURES_PARAMS, FuturesParams

SYMBOL = "BTCUSDT"
DAY_MS = 86_400_000
CONTRACT_KINDS = ("global_accounts", "top_accounts", "top_positions")


@pytest.fixture
def fake(fake_binance, fake_time):
    fake_binance.clock = fake_time.clock
    return fake_binance


def client_for(fake, fake_time, **kwargs) -> BinanceFuturesClient:
    budget = kwargs.pop("budget", None) or RequestBudget(clock=fake_time.clock, sleep=fake_time.sleep)
    return BinanceFuturesClient(fake.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock, **kwargs)


def now_ms(fake_time) -> int:
    return int(fake_time.clock() * 1000)


def latest_closed(fake_time) -> int:
    """The end of the newest period that has closed: periods starting before it are out."""
    return now_ms(fake_time) // PERIOD_MS * PERIOD_MS


async def collect(pages):
    out = []
    async for page in pages:
        assert page, "no empty pages"
        out.append(page)
    return out


def ms(event) -> int:
    return int(event.event_time.timestamp() * 1000)


@pytest.mark.parametrize("kind", FUTURES_RATIO_KINDS)
async def test_each_ratio_pages_a_span_oldest_first_as_the_perpetuals(fake, fake_time, kind):
    end = latest_closed(fake_time)
    start = end - 2 * DAY_MS  # 576 periods: a page of 500, then 76
    async with client_for(fake, fake_time) as client:
        pages = await collect(client.ratios(SYMBOL, kind, start, end))

    assert [len(page) for page in pages] == [PAGE_LIMIT, 576 - PAGE_LIMIT]
    events = [event for page in pages for event in page]
    assert [ms(e) for e in events] == list(range(start, end, PERIOD_MS))
    for event in events[:3] + events[-3:]:
        ratio, long, short = fake.ratios.values(SYMBOL, kind, "PERPETUAL", ms(event))
        assert (event.ratio, event.long_value, event.short_value) == (Decimal(ratio), Decimal(long), Decimal(short))
        assert (event.symbol, event.kind, event.period, event.contract_type) == (SYMBOL, kind, "5m", "PERPETUAL")
        assert event.exchange == "binance"

    requests = fake.requests_to(PATHS[kind])
    assert len(requests) == 2
    # Both ends every time, a page's worth apart: never more than Binance returns at once.
    assert [(int(r["startTime"]), int(r["endTime"])) for r in requests] == [
        (start, start + PAGE_LIMIT * PERIOD_MS - 1),
        (start + PAGE_LIMIT * PERIOD_MS, end - 1),
    ]
    assert all(r["period"] == "5m" and r["limit"] == str(PAGE_LIMIT) for r in requests)


async def test_contract_type_perpetual_goes_wherever_an_endpoint_takes_it(fake, fake_time):
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        for kind in FUTURES_RATIO_KINDS:
            await collect(client.ratios(SYMBOL, kind, end - 3 * PERIOD_MS, end))

    for kind in CONTRACT_KINDS:
        (request,) = fake.requests_to(PATHS[kind])
        assert request["contractType"] == "PERPETUAL"
    (taker,) = fake.requests_to(PATHS["taker_volume"])
    assert "contractType" not in taker and taker["symbol"] == SYMBOL
    # The August 2026 change log names the top traders' accounts' contract by pair.
    assert fake.requests_to(PATHS["top_accounts"])[0]["pair"] == SYMBOL


async def test_values_are_the_perpetuals_not_every_contract_combined(fake, fake_time):
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        (page,) = await collect(client.ratios(SYMBOL, "global_accounts", end - PERIOD_MS, end))
    perpetual = fake.ratios.values(SYMBOL, "global_accounts", "PERPETUAL", end - PERIOD_MS)
    combined = fake.ratios.values(SYMBOL, "global_accounts", "ALL", end - PERIOD_MS)
    assert perpetual != combined
    assert str(page[0].ratio) == perpetual[0]


async def test_a_refused_shape_falls_through_to_the_next_and_the_one_taken_is_kept(fake, fake_time, caplog):
    # This Binance names global accounts' contract by pair.
    fake.futures_params["global_accounts"] = FuturesParams("pair", True)
    end = latest_closed(fake_time)
    with caplog.at_level(logging.INFO, logger="binance_futures_rest"):
        async with client_for(fake, fake_time) as client:
            await collect(client.ratios(SYMBOL, "global_accounts", end - 2 * PERIOD_MS, end))
            await collect(client.ratios(SYMBOL, "global_accounts", end - PERIOD_MS, end))
            assert client.shape("global_accounts") == ParamShape("pair", True)

    first, second, third = fake.requests_to(PATHS["global_accounts"])
    assert "symbol" in first and "pair" not in first  # refused: -1104, a parameter it doesn't read
    assert second["pair"] == SYMBOL and second["contractType"] == "PERPETUAL"
    assert third["pair"] == SYMBOL  # settled: no more probing
    assert "asking by pair + contractType=PERPETUAL" in caplog.text


async def test_before_contract_types_existed_symbol_alone_is_the_perpetual(fake, fake_time, caplog):
    fake.futures_params = dict(LEGACY_FUTURES_PARAMS)
    end = latest_closed(fake_time)
    with caplog.at_level(logging.INFO, logger="binance_futures_rest"):
        async with client_for(fake, fake_time) as client:
            for kind in FUTURES_RATIO_KINDS:
                (page,) = await collect(client.ratios(SYMBOL, kind, end - PERIOD_MS, end))
                assert str(page[0].ratio) == fake.ratios.values(SYMBOL, kind, "PERPETUAL", end - PERIOD_MS)[0]
                assert client.shape(kind) == ParamShape("symbol", False)

    # Every account endpoint tried both contractType shapes before symbol alone.
    for kind in CONTRACT_KINDS:
        assert len(fake.requests_to(PATHS[kind])) == 3
    assert "takes no contractType" in caplog.text


async def test_no_shape_taken_is_an_error_naming_every_refusal(fake, fake_time):
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        with pytest.raises(BinanceRequestError, match="took none of its parameter shapes") as raised:
            await collect(client.ratios("BTCBUSD", "global_accounts", end - PERIOD_MS, end))
    detail = raised.value.detail
    for shape in ("symbol + contractType=PERPETUAL:", "pair + contractType=PERPETUAL:", "symbol alone:"):
        assert shape in detail
    assert "Invalid symbol" in detail


async def test_only_the_latest_30_days_exist(fake, fake_time):
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        pages = await collect(client.ratios(SYMBOL, "taker_volume", end - 40 * DAY_MS, end))
    events = [event for page in pages for event in page]
    assert ms(events[0]) >= now_ms(fake_time) - 30 * DAY_MS
    assert ms(events[-1]) == end - PERIOD_MS
    assert len(events) == 30 * 288 or len(events) == 30 * 288 - 1  # the 30-day edge falls inside a period


async def test_a_period_not_yet_published_is_not_there(fake, fake_time):
    fake.publish_delay_ms = 60_000
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        pages = await collect(client.ratios(SYMBOL, "taker_volume", end - 2 * PERIOD_MS, end))
    newest = ms(pages[-1][-1])
    # The period that closed less than a minute ago may still be missing; nothing newer ever appears.
    assert newest <= end - PERIOD_MS


async def test_holes_are_skipped(fake, fake_time):
    end = latest_closed(fake_time)
    fake.ratios.holes = [("top_positions", end - 2 * PERIOD_MS)]
    async with client_for(fake, fake_time) as client:
        (page,) = await collect(client.ratios(SYMBOL, "top_positions", end - 3 * PERIOD_MS, end))
    assert [ms(e) for e in page] == [end - 3 * PERIOD_MS, end - PERIOD_MS]


async def test_a_429_pauses_for_its_retry_after_without_counting_as_a_failure(fake, fake_time):
    fake.fail_next(429, retry_after=42, count=3)
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time, max_attempts=1) as client:
        (page,) = await collect(client.ratios(SYMBOL, "taker_volume", end - PERIOD_MS, end))
    assert len(page) == 1
    assert fake_time.sleeps.count(42) == 3


async def test_a_418_is_a_ban(fake, fake_time):
    fake.fail_next(418, retry_after=120)
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        with pytest.raises(BinanceBanned) as raised:
            await collect(client.ratios(SYMBOL, "taker_volume", end - PERIOD_MS, end))
    assert raised.value.retry_after == 120


async def test_server_errors_retry_with_backoff_then_give_up(fake, fake_time):
    fake.fail_next(500, count=2)
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        (page,) = await collect(client.ratios(SYMBOL, "taker_volume", end - PERIOD_MS, end))
    assert len(page) == 1 and fake_time.sleeps[:2] == [1.0, 2.0]

    fake.fail_next(503, count=3)
    async with client_for(fake, fake_time, max_attempts=3) as client:
        with pytest.raises(BinanceUnavailable, match="3 attempts"):
            await collect(client.ratios(SYMBOL, "taker_volume", end - PERIOD_MS, end))


async def test_any_other_refusal_fails_at_once_even_while_probing(fake, fake_time):
    fake.fail_next(403, msg="WAF limit")
    end = latest_closed(fake_time)
    async with client_for(fake, fake_time) as client:
        with pytest.raises(BinanceRequestError, match="WAF limit"):
            await collect(client.ratios(SYMBOL, "global_accounts", end - PERIOD_MS, end))
    assert len(fake.requests_to(PATHS["global_accounts"])) == 1


async def test_the_budget_holds_requests_to_its_window(fake_time):
    budget = RequestBudget(per_window=3, window_seconds=300, clock=fake_time.clock, sleep=fake_time.sleep)
    start = fake_time.clock()
    for _ in range(3):
        await budget.acquire()
    assert fake_time.clock() == start
    await budget.acquire()  # the fourth waits until the first leaves the window
    assert fake_time.clock() == start + 300

    with pytest.raises(ValueError):
        RequestBudget(per_window=1001)


def test_a_row_reads_timestamps_as_numbers_or_strings_and_either_share_naming():
    client = BinanceFuturesClient("http://127.0.0.1:9")
    row = {"symbol": SYMBOL, "longShortRatio": "1.5", "longPosition": "0.6", "shortPosition": "0.4", "timestamp": "1800000000000"}
    event = client._event(SYMBOL, "top_positions", row)
    assert (event.ratio, event.long_value, event.short_value) == (Decimal("1.5"), Decimal("0.6"), Decimal("0.4"))
    assert ms(event) == 1_800_000_000_000
    assert client._event(SYMBOL, "taker_volume", {"buySellRatio": "1.2", "timestamp": 1}) is None  # malformed: skipped
