"""
bar_series.py

The bars a strategy steps through: Bronze's 1m klines (lake_history.py)
rolled up into one chart timeframe, each with its EMAs from Spark's own
streaming/indicator_engine.py, so a strategy sees the values the chart
shows (Phase 4b's kickoff choices, docs/ROADMAP.md).

**Bars** are epoch-aligned buckets, floor(t / size) * size, exactly as
backend/postgres_client.py rolls candles_1m up for the chart: open from the
bucket's first minute, close from its last, high and low the extremes,
volume the sum. So a bar here is the bar on the chart, minute for minute,
wherever `candles_1m` holds the exchange's klines (everywhere the
backfill has been).

**EMAs** come from an IndicatorEngine fed every stored minute from the
oldest, the way Spark's warm-up feeds it `candles_1m`: an EMA depends on
every bar before it, so starting it at a backtest's first bar would seed it
differently from the chart's. The engine runs settled at once and final
only (as for Gold's indicators: these minutes are the exchange's and can't
change), so it emits each bar once its last minute is in; a bar it never
emits is the one still forming where the data ends, and is dropped, never
traded. The engine always computes the server's lengths
(config/settings.py's SERVER_EMA_LENGTHS) besides the strategy's, so every
bar can be checked against Spark's `candle_indicators`
(indicator_check.py); lengths only change their own values.

Author: @DS
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from config.settings import SERVER_EMA_LENGTHS, TIMEFRAMES

from indicator_engine import IndicatorEngine
from lake_history import Level, Minutes

TIMEFRAME_SECONDS: Dict[str, int] = {tf: int(size.total_seconds()) for tf, size in TIMEFRAMES.items()}


@dataclass(frozen=True)
class Bar:
    """One bar of the run's timeframe, as the chart draws it, with its EMAs."""

    time: int  # Unix seconds, the bar's start (the chart's bar time)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    ema: Mapping[int, Optional[float]]  # EMA length -> value at this bar's close; None while seeding
    # (price, buy volume, sell volume) per level, prices ascending: only for a
    # strategy that asks for it, and only where Bronze's aggTrades cover the bar.
    footprint: Optional[Tuple[Level, ...]] = None


def build_bars(minutes: Minutes, timeframe: str, ema_lengths: Iterable[int] = ()) -> List[Bar]:
    """Every finished bar of `timeframe` in `minutes`, oldest first, with EMAs at `ema_lengths` and the server's."""
    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError(f"timeframe must be one of {list(TIMEFRAME_SECONDS)}, not {timeframe!r}")
    seconds = TIMEFRAME_SECONDS[timeframe]
    lengths = tuple(sorted(set(ema_lengths) | set(SERVER_EMA_LENGTHS)))
    emas = _emas(minutes, timeframe, seconds, lengths)

    bars: List[Bar] = []
    current: Optional[list] = None  # [start, open, high, low, close, volume]
    for i, minute in enumerate(minutes.start):
        start = minute - minute % seconds
        if current is None or start != current[0]:
            if current is not None and current[0] in emas:
                bars.append(Bar(*current, ema=emas[current[0]]))
            current = [start, minutes.open[i], minutes.high[i], minutes.low[i], minutes.close[i], minutes.volume[i]]
        else:
            current[2] = max(current[2], minutes.high[i])
            current[3] = min(current[3], minutes.low[i])
            current[4] = minutes.close[i]
            current[5] += minutes.volume[i]
    if current is not None and current[0] in emas:
        bars.append(Bar(*current, ema=emas[current[0]]))
    return bars


def with_footprints(bars: List[Bar], footprints: Mapping[int, Tuple[Level, ...]]) -> List[Bar]:
    """`bars` with each covered bar's footprint (lake_history.read_footprint())."""
    return [
        Bar(b.time, b.open, b.high, b.low, b.close, b.volume, b.ema, footprints[b.time]) if b.time in footprints else b
        for b in bars
    ]


def _emas(minutes: Minutes, timeframe: str, seconds: int, lengths: Tuple[int, ...]) -> Dict[int, Dict[int, Optional[float]]]:
    """Bar start -> {length: EMA} for every bar the engine finalized."""
    engine = IndicatorEngine(settle_minutes=0, final_only=True, lengths=lengths, timeframes={timeframe: seconds})
    values: Dict[int, Dict[int, Optional[float]]] = {}
    for i, (minute, close) in enumerate(zip(minutes.start, minutes.close)):
        engine.update(minute, str(close))
        if i % 1440 == 0:
            values.update((row.window_start, row.ema) for row in engine.drain())
    values.update((row.window_start, row.ema) for row in engine.drain())
    return values
