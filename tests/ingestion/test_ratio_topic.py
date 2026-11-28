"""
Tests for ingestion/ratio_topic.py over real Kafka, and the Done-when of
Phase 4c it serves: a restarted poller fills in the periods it missed,
reading where it left off back from market.futures.ratios. The poller runs
against a fake Binance on a fake clock (tests/fake_binance.py), publishing
through ingestion's real MarketDataProducer.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from binance_futures_rest import PERIOD_MS, BinanceFuturesClient, RequestBudget
from config.settings import FUTURES_RATIO_KINDS, FUTURES_RATIOS_TOPIC
from futures_poller import FuturesRatioPoller
from helpers import TopicTail
from ratio_topic import newest_periods
from schema import FuturesRatio

DAY_MS = 86_400_000


class Stop(Exception):
    pass


def ratio(symbol: str, kind: str, start_ms: int) -> FuturesRatio:
    return FuturesRatio(
        exchange="binance",
        symbol=symbol,
        event_time=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
        kind=kind,
        ratio=Decimal("1.25"),
        long_value=Decimal("0.5556"),
        short_value=Decimal("0.4444"),
    )


async def test_the_newest_period_of_each_ratio_is_found_among_other_symbols(trade_producer, symbol):
    other = symbol.replace("USDT", "XUSDT")
    base = 1_800_000_000_000
    events = [ratio(symbol, "taker_volume", base + i * PERIOD_MS) for i in range(5)]
    events += [ratio(other, "taker_volume", base + i * PERIOD_MS) for i in range(50)]
    events += [ratio(symbol, "global_accounts", base + 7 * PERIOD_MS)]
    # A refetch republishing an older period after the newest: the newest still wins.
    events += [ratio(symbol, "taker_volume", base + PERIOD_MS)]
    events += [ratio(other, "global_accounts", base + i * PERIOD_MS) for i in range(30)]
    await trade_producer.send_batch(events)

    wanted = [(symbol, kind) for kind in FUTURES_RATIO_KINDS]
    for chunk in (2_000, 7):  # all at once, and many chunks back from the end
        found = await newest_periods(wanted, chunk=chunk)
        assert found == {(symbol, "taker_volume"): base + 4 * PERIOD_MS, (symbol, "global_accounts"): base + 7 * PERIOD_MS}


async def test_a_symbol_never_published_has_nothing_to_resume_from(kafka_topics, symbol):
    assert await newest_periods([(symbol, "taker_volume")], chunk=500, max_scan=5_000) == {}


async def run_poller(fake, fake_time, producer, symbol, run_for, resume_from):
    horizon = fake_time.clock() + run_for

    async def sleep(seconds):
        await fake_time.sleep(seconds)
        if fake_time.clock() >= horizon:
            # Every follower moves the one fake clock: let any still fetching finish first.
            await asyncio.sleep(0.3)
            raise Stop

    budget = RequestBudget(clock=fake_time.clock, sleep=fake_time.sleep)
    async with BinanceFuturesClient(fake.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock) as client:
        poller = FuturesRatioPoller(
            client,
            [symbol],
            producer.send_batch,
            resume_from=resume_from,
            history_days=0.25,
            clock=fake_time.clock,
            sleep=sleep,
        )
        with pytest.raises(Stop):
            await poller.run()


async def test_a_restarted_poller_fills_in_the_periods_it_missed(fake_binance, fake_time, trade_producer, symbol):
    fake_binance.clock = fake_time.clock
    wanted = [(symbol, kind) for kind in FUTURES_RATIO_KINDS]
    async with TopicTail(FUTURES_RATIOS_TOPIC) as tail:
        # First run: six hours of history (history_days=0.25), then 15 minutes live.
        await run_poller(fake_binance, fake_time, trade_producer, symbol, 15 * 60, resume_from=None)
        first = await newest_periods(wanted)
        assert set(first) == set(wanted)

        # Off for two hours, then started again from what the topic holds.
        fake_time.now += 2 * 3600
        restart = int(fake_time.clock() * 1000)
        await run_poller(fake_binance, fake_time, trade_producer, symbol, 60, resume_from=first)
        messages = await tail.collect_json(lambda m: m["symbol"] == symbol)

    # Every period that had closed when it restarted (the followers share the
    # fake clock, so it ends wherever their sleeps took it).
    closed_at_restart = restart // PERIOD_MS * PERIOD_MS
    for kind in FUTURES_RATIO_KINDS:
        got = [m for m in messages if m["kind"] == kind]
        starts = [int(datetime.fromisoformat(m["event_time"].replace("Z", "+00:00")).timestamp() * 1000) for m in got]
        assert starts == list(range(starts[0], starts[-1] + PERIOD_MS, PERIOD_MS)), f"{kind}: a gap or a repeat"
        assert starts[-1] >= closed_at_restart - PERIOD_MS
        assert starts[0] <= restart - 2 * 3600 * 1000 - 5 * 3600 * 1000  # the six hours before the first run's end
        values = fake_binance.ratios.values(symbol, kind, "PERPETUAL", starts[-1])
        assert (got[-1]["ratio"], got[-1]["long_value"], got[-1]["short_value"]) == values
        assert got[-1]["event_type"] == "futures_ratio" and got[-1]["contract_type"] == "PERPETUAL"
    assert all(json.dumps(m) for m in messages)
