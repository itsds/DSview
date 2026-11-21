"""
Tests for streaming/indicator_engine.py: the EMA engine on its own (no
Spark), including the golden tests against the browser's math.js.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone

import pytest

from candle_windowing import WATERMARK_DELAY
from config.settings import SERVER_EMA_LENGTHS
from indicator_engine import (
    LIVE_SETTLE_MINUTES,
    TIMEFRAME_SECONDS,
    IndicatorEngine,
    IndicatorRow,
    StateMismatch,
    ema_step,
)

from helpers import golden_minutes, math_js_ema

# A UTC midnight, so every timeframe's buckets (1D included) start with the series.
BASE = datetime(2026, 11, 20, tzinfo=timezone.utc)
# EMA 200 is defined on 1m, 5m (600 bars) and 15m (200 bars); 1h, 4h and 1D
# get a handful of bars each, enough to check their bucketing.
GOLDEN_COUNT = 3000

ONE_MINUTE_ONLY = {"1m": 60}


def _values(rows):
    return [(row.timeframe, row.window_start, row.ema) for row in rows]


def test_ema_step_follows_math_js():
    # math.test.js's worked example: seed (1 + 2 + 3) / 3 = 2, then 0.5 * 4 + 0.5 * 2 = 3, ...
    state, out = (0.0, 0, None), []
    for close in [1, 2, 3, 4, 5]:
        state = ema_step(state, float(close), 3)
        out.append(state[2])
    assert out == [None, None, 2, 3, 4]


def test_settle_window_outlasts_the_candle_watermark():
    # LIVE_SETTLE_MINUTES = 2 is only right while a 1m candle stops changing
    # less than a minute after its window closes.
    (amount,) = re.fullmatch(r"(\d+) seconds?", WATERMARK_DELAY).groups()
    assert int(amount) < (LIVE_SETTLE_MINUTES - 1) * 60


def test_minutes_must_be_aligned():
    with pytest.raises(ValueError):
        IndicatorEngine().update(90, "1")


def test_a_bar_takes_the_close_of_its_latest_minute():
    engine = IndicatorEngine(settle_minutes=0, final_only=True, lengths=(1,), timeframes={"5m": 300})
    for minute, close in [(0, "1"), (180, "4"), (60, "2"), (300, "6"), (540, "10")]:
        engine.update(minute, close)
    # Bar 0 closes at minute 180's close: minute 60 arriving after it doesn't
    # move it, and minute 300 (the next bar) makes it final. EMA 1 is just the close.
    assert _values(engine.drain()) == [("5m", 0, {1: 4.0}), ("5m", 300, {1: 10.0})]
    assert engine.ignored == 0


def test_a_live_bar_is_provisional_until_it_settles():
    engine = IndicatorEngine(lengths=(2,), timeframes=ONE_MINUTE_ONLY)
    engine.update(0, "10")
    assert _values(engine.drain()) == [("1m", 0, {2: None})]  # still seeding

    engine.update(60, "20")
    engine.update(60, "30")  # the same minute again, as it forms
    assert _values(engine.drain()) == [("1m", 60, {2: 20.0})]  # (10 + 30) / 2

    engine.update(60, "30")  # re-sent unchanged (only its volume moved): nothing to say
    assert engine.drain() == []

    engine.update(0, "0")  # a late trade revises the previous minute: it and everything after it change
    assert _values(engine.drain()) == [("1m", 0, {2: None}), ("1m", 60, {2: 15.0})]

    engine.update(120, "40")  # minute 0 is now two minutes old: final, unchanged, so not re-sent
    assert _values(engine.drain()) == [("1m", 120, {2: 2 / 3 * 40 + (1 - 2 / 3) * 15.0})]

    engine.update(0, "5")  # can't happen with a 10 s watermark; ignored and counted
    assert engine.drain() == []
    assert engine.ignored == 1


def test_final_only_sends_each_bar_once_when_it_is_final():
    engine = IndicatorEngine(lengths=(1,), timeframes=ONE_MINUTE_ONLY, final_only=True)
    for minute, close in [(0, "1"), (0, "2"), (60, "3"), (120, "4")]:
        engine.update(minute, close)
    assert _values(engine.drain()) == [("1m", 0, {1: 2.0})]  # 60 and 120 can still change
    engine.update(180, "5")
    assert _values(engine.drain()) == [("1m", 60, {1: 3.0})]


def test_saved_state_carries_on_exactly():
    minutes = golden_minutes(400, BASE)
    uninterrupted, restored = IndicatorEngine(), IndicatorEngine()
    rows_a, rows_b = [], []
    for i, (minute, close) in enumerate(minutes):
        uninterrupted.update(minute, close)
        restored.update(minute, close)
        if i % 13 == 0:
            rows_a += uninterrupted.drain()
            rows_b += restored.drain()
            # Through JSON, the way indicator_state.py keeps it in Spark.
            restored = IndicatorEngine.from_state(json.loads(json.dumps(restored.to_state())))
    rows_a += uninterrupted.drain()
    rows_b += restored.drain()
    assert rows_a == rows_b


def test_saving_undrained_rows_is_refused():
    engine = IndicatorEngine()
    engine.update(0, "1")
    with pytest.raises(RuntimeError):
        engine.to_state()


def test_state_from_another_configuration_is_refused():
    saved = IndicatorEngine(lengths=(20,)).to_state()
    with pytest.raises(StateMismatch, match="checkpoint"):
        IndicatorEngine.from_state(saved, lengths=(20, 50))
    with pytest.raises(StateMismatch):
        IndicatorEngine.from_state({**saved, "version": 0}, lengths=(20,))


def _last_values(rows):
    """{timeframe: {length: {bar start: the last value sent for it}}}."""
    last = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        for period, value in row.ema.items():
            last[row.timeframe][period][row.window_start] = value
    return last


def test_golden_live_stream_converges_on_the_browsers_ema():
    """
    The live engine, fed candles the way candle_aggregator.py publishes them
    (each minute several times as it forms, now and then a late revision of
    the previous one), ends with exactly math.js's EMA of the final candles
    on every bar of every timeframe — to the last bit.
    """
    minutes = golden_minutes(GOLDEN_COUNT, BASE)
    engine = IndicatorEngine()
    rows = []
    for i, (minute, close) in enumerate(minutes):
        engine.update(minute, str(float(close) + 3.5))  # still forming
        engine.update(minute, close)
        if i % 7 == 3:
            previous_minute, previous_close = minutes[i - 1]
            engine.update(previous_minute, str(float(previous_close) - 1))  # a late trade
            engine.update(previous_minute, previous_close)  # and the candle as it finally is
        if i % 5 == 0:
            rows += engine.drain()
    rows += engine.drain()
    assert engine.ignored == 0

    expected = math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS)
    got = _last_values(rows)
    for timeframe in TIMEFRAME_SECONDS:
        for period in SERVER_EMA_LENGTHS:
            assert got[timeframe][period] == expected[timeframe][period], (timeframe, period)
    # The series really does reach every length on the short timeframes.
    assert expected["15m"][200][max(expected["15m"][200])] is not None


def test_golden_final_candles_give_the_browsers_ema_once_per_bar():
    """Gold's mode: final candles in, each bar out exactly once, equal to math.js."""
    minutes = golden_minutes(GOLDEN_COUNT, BASE)
    engine = IndicatorEngine(settle_minutes=0, final_only=True)
    rows = []
    for i, (minute, close) in enumerate(minutes):
        engine.update(minute, close)
        if i % 11 == 0:
            rows += engine.drain()
    rows += engine.drain()

    keys = [(row.timeframe, row.window_start) for row in rows]
    assert len(keys) == len(set(keys))
    expected = math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS)
    got = _last_values(rows)
    last_minute = minutes[-1][0]
    for timeframe, seconds in TIMEFRAME_SECONDS.items():
        # Every bar that has seen its last minute is out; the rest are still open.
        final_starts = [s for s in expected[timeframe][20] if s + seconds - 60 <= last_minute]
        for period in SERVER_EMA_LENGTHS:
            assert got[timeframe][period] == {s: expected[timeframe][period][s] for s in final_starts}, (
                timeframe,
                period,
            )


def test_rows_are_plain_data():
    row = IndicatorRow("1m", 0, {20: None})
    assert row == IndicatorRow("1m", 0, {20: None})
