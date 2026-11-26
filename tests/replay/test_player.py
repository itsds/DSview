"""Tests for replay/player.py: trades go out as the clock reaches them; pause, speed and gaps behave."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from decimal import Decimal

import pyarrow as pa
import pytest

import player as player_module
from bronze_source import TRADE_SCHEMA
from clock import MAX, ReplayClock
from player import ReplayPlayer

from bronze_fakes import T0


def _table(seconds):
    return pa.table(
        {
            "event_time": [T0 + timedelta(seconds=s) for s in seconds],
            "trade_id": [str(i) for i in range(len(seconds))],
            "price": [Decimal("97000.5")] * len(seconds),
            "quantity": [Decimal("0.25")] * len(seconds),
            "side": ["buy", "sell"] * (len(seconds) // 2) + ["buy"] * (len(seconds) % 2),
            "exchange": ["binance"] * len(seconds),
        },
        schema=TRADE_SCHEMA,
    )


class Recorder:
    """A `send` that records each batch with the clock's time when it was sent."""

    def __init__(self) -> None:
        self.batches = []
        self.clock = None

    async def __call__(self, events) -> None:
        self.batches.append((self.clock.time(), events))

    @property
    def events(self):
        return [e for _, batch in self.batches for e in batch]


def _player(tables, speed, recorder):
    clock = ReplayClock(T0, speed)
    recorder.clock = clock
    return ReplayPlayer(iter(tables), clock, recorder, "BTCUSDT")


async def test_at_max_speed_everything_goes_in_order_in_bounded_batches(monkeypatch):
    monkeypatch.setattr(player_module, "BATCH_LIMIT", 4)
    recorder = Recorder()
    p = _player([_table(range(10)), _table(range(10, 15))], MAX, recorder)
    p.play()
    await asyncio.wait_for(p.run(), 5)

    times = [e.event_time for e in recorder.events]
    assert times == [T0 + timedelta(seconds=s) for s in range(15)]
    assert max(len(batch) for _, batch in recorder.batches) <= 4
    assert p.trades_sent == 15 and p.clock.time() == T0 + timedelta(seconds=14)


async def test_events_are_the_trades_as_ingestion_would_publish_them():
    recorder = Recorder()
    p = _player([_table([0, 1])], MAX, recorder)
    p.play()
    await p.run()
    event = recorder.events[1]
    assert (event.symbol, event.trade_id, event.price, event.quantity, event.side, event.exchange) == (
        "BTCUSDT", "1", Decimal("97000.5"), Decimal("0.25"), "sell", "binance"
    )
    assert event.model_dump(mode="json")["event_type"] == "trade"


async def test_no_trade_goes_out_before_the_clock_reaches_it():
    recorder = Recorder()
    p = _player([_table([0, 0.5, 1.0, 1.5, 2.0])], 10, recorder)  # 2 s of trades in ~0.2 s
    p.play()
    started = time.monotonic()
    await asyncio.wait_for(p.run(), 5)

    assert time.monotonic() - started >= 0.18
    for clock_time, batch in recorder.batches:
        assert all(event.event_time <= clock_time for event in batch)
    assert len(recorder.events) == 5


async def test_a_paused_player_sends_nothing_until_resumed():
    recorder = Recorder()
    p = _player([_table([0, 0.2, 0.4, 0.6])], 1, recorder)
    p.play()
    run = asyncio.create_task(p.run())
    await asyncio.sleep(0.05)
    p.pause()
    sent = len(recorder.events)
    await asyncio.sleep(0.8)  # long enough for every trade, had it kept playing
    assert len(recorder.events) == sent < 4

    p.play()
    await asyncio.wait_for(run, 5)
    assert len(recorder.events) == 4


async def test_a_faster_speed_wakes_a_waiting_player():
    recorder = Recorder()
    p = _player([_table([0, 50])], 1, recorder)  # 50 s apart: under GAP_SKIP, so really waited for
    p.play()
    run = asyncio.create_task(p.run())
    await asyncio.sleep(0.1)
    assert len(recorder.events) == 1
    p.set_speed(MAX)
    await asyncio.wait_for(run, 2)
    assert len(recorder.events) == 2


async def test_a_hole_in_the_recording_is_skipped_not_waited_through():
    recorder = Recorder()
    p = _player([_table([0, 3600, 3601])], 1, recorder)
    p.play()
    await asyncio.wait_for(p.run(), 5)  # an hour at 1x, were it waited through
    assert len(recorder.events) == 3


async def test_an_error_sending_ends_the_run():
    async def broken(events):
        raise ConnectionError("broker gone")

    p = ReplayPlayer(iter([_table([0])]), ReplayClock(T0, MAX), broken, "BTCUSDT")
    p.play()
    with pytest.raises(ConnectionError):
        await p.run()
