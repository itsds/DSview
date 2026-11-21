"""
indicator_engine.py

EMA 20, 50 and 200 on every chart timeframe, from a stream of 1-minute
candle updates — what jobs/indicators.py (live) and jobs/storage_writer.py
(Gold) run inside Spark's applyInPandasWithState, one engine per symbol,
its state saved by Spark between micro-batches (indicator_state.py is the
Spark side). Plain Python with no Spark in it, so it's tested on its own
and against the browser's own formulas (tests/streaming/
test_indicator_engine.py runs frontend/src/indicators/math.js itself).

**The formula** is math.js's ema(), step for step and in the same
floating-point operation order, so on identical candles the two agree to
the last bit: seeded with the plain average of the first `period` closes,
then value = α · close + (1 − α) · previous, with α = 2 / (period + 1).

**Buckets.** Each timeframe's bars are rolled up from 1m candles in
buckets aligned to the Unix epoch — floor(t / size) · size — exactly as
backend/postgres_client.py's date_bin() history and the frontend's live
roll-up do (config/settings.py's TIMEFRAMES). An EMA only needs each bar's
close, the close of the latest 1m candle in its bucket, so that is all a
bucket keeps: state stays a few numbers per timeframe however long a day
bar is.

**Provisional and final bars.** The live input is candle_aggregator.py's
update-mode output, through the market.candles.1m topic: the same 1m
candle arrives again and again while it forms, and a late trade can still
revise it after the next minute has started. So a bar's value is
provisional, recomputed from the state before it whenever its close
changes, until nothing can change it any more; only then is it folded
into the running state ("final"). A 1m candle can only be revised while
its window is within candle_windowing.WATERMARK_DELAY (10 s) of the newest
trade, and candle_aggregator.py publishes each micro-batch's windows
oldest first, so once a candle two minutes newer has been seen it can't
change again: LIVE_SETTLE_MINUTES. Gold's input is finalized candles, so
there a minute is settled as soon as it arrives (settle_minutes=0).

**What comes out.** drain() returns a row for every bar whose value may
have changed since the last drain. Live: the changed bar and every bar
after it (an EMA depends on every bar before it), provisional bars
included — repeated upserts converge on the final value, like
postgres_sink.py's candles. With final_only=True (Gold, an append-only
sink): each bar exactly once, when it becomes final.

Author: @DS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from config.settings import SERVER_EMA_LENGTHS, TIMEFRAMES

MINUTE_SECONDS = 60
TIMEFRAME_SECONDS: Dict[str, int] = {tf: int(size.total_seconds()) for tf, size in TIMEFRAMES.items()}

# Minutes after which a live 1m candle can no longer change (see the module
# docstring). Holds for any candle watermark under a minute;
# tests/streaming/test_indicator_engine.py checks candle_windowing.py's.
LIVE_SETTLE_MINUTES = 2

# Bump when to_state()'s layout changes. A checkpoint saved by another
# version (or with other lengths, timeframes or settling) is refused rather
# than misread: see StateMismatch.
STATE_VERSION = 1

# (sum of the closes seen while seeding, how many, the EMA once seeded).
EmaState = Tuple[float, int, Optional[float]]
EMPTY_EMA: EmaState = (0.0, 0, None)


def ema_step(state: EmaState, close: float, period: int) -> EmaState:
    """One bar of math.js's ema(): its smooth() with α = 2 / (period + 1), in the same operation order."""
    seed_sum, seed_count, value = state
    if value is None:
        seed_sum += close
        seed_count += 1
        if seed_count == period:
            value = seed_sum / period
        return (seed_sum, seed_count, value)
    alpha = 2 / (period + 1)
    return (seed_sum, seed_count, alpha * close + (1 - alpha) * value)


@dataclass(frozen=True)
class IndicatorRow:
    """One bar's indicator values: EMA length -> value (None while that EMA is still seeding)."""

    timeframe: str
    window_start: int  # Unix seconds, the bucket's start
    ema: Dict[int, Optional[float]]


class StateMismatch(ValueError):
    """A saved state from a different engine version or configuration."""


@dataclass
class _Bucket:
    start: int  # Unix seconds, aligned to the timeframe
    minute: int  # start of the latest 1m candle seen in it
    close: str  # that candle's close, exactly as published


class _Timeframe:
    def __init__(self, seconds: int, lengths: Sequence[int]) -> None:
        self.seconds = seconds
        self.base: Dict[int, EmaState] = {period: EMPTY_EMA for period in lengths}  # after every final bar
        self.pending: List[_Bucket] = []  # not final yet, oldest first: usually one or two
        self.final_through: Optional[int] = None  # start of the newest final bar
        self.dirty_from: Optional[int] = None  # bars from this start on changed since the last drain

    def mark_dirty(self, start: int) -> None:
        self.dirty_from = start if self.dirty_from is None else min(self.dirty_from, start)


class IndicatorEngine:
    """The running EMAs of one symbol, on every timeframe; fed 1m candles in the order they were published."""

    def __init__(
        self,
        settle_minutes: int = LIVE_SETTLE_MINUTES,
        final_only: bool = False,
        lengths: Sequence[int] = SERVER_EMA_LENGTHS,
        timeframes: Mapping[str, int] = TIMEFRAME_SECONDS,
    ) -> None:
        self._settle_minutes = settle_minutes
        self._final_only = final_only
        self._lengths = tuple(lengths)
        self._timeframe_seconds = dict(timeframes)
        self._frames = {tf: _Timeframe(seconds, self._lengths) for tf, seconds in self._timeframe_seconds.items()}
        self._newest_minute: Optional[int] = None
        self._out: List[IndicatorRow] = []
        # Updates that arrived after their bar was final, which settling rules
        # out; counted so the caller can report one rather than lose it silently.
        self.ignored = 0

    def update(self, minute: int, close: str) -> None:
        """Fold in one version of the 1m candle starting at `minute` (Unix seconds) with close `close`."""
        if minute % MINUTE_SECONDS:
            raise ValueError(f"not the start of a minute: {minute}")
        if self._newest_minute is None or minute > self._newest_minute:
            self._newest_minute = minute
        late = False
        for timeframe, frame in self._frames.items():
            late |= not self._apply(frame, minute, close)
            self._finalize(timeframe, frame)
        self.ignored += late

    def drain(self) -> List[IndicatorRow]:
        """Rows for every bar that may have changed since the last drain; oldest first within each timeframe."""
        rows, self._out = self._out, []
        for timeframe, frame in self._frames.items():
            if frame.dirty_from is not None and not self._final_only:
                state = dict(frame.base)
                for bucket in frame.pending:
                    state = {period: ema_step(state[period], float(bucket.close), period) for period in self._lengths}
                    if bucket.start >= frame.dirty_from:
                        rows.append(IndicatorRow(timeframe, bucket.start, {p: s[2] for p, s in state.items()}))
            frame.dirty_from = None
        return rows

    def to_state(self) -> Dict[str, Any]:
        """A JSON-able snapshot (Spark keeps it between micro-batches). Drain first."""
        if self._out or any(frame.dirty_from is not None for frame in self._frames.values()):
            raise RuntimeError("drain() before saving: undrained rows would be lost")
        return {
            "version": STATE_VERSION,
            "config": self._config(),
            "newest_minute": self._newest_minute,
            "frames": {
                timeframe: {
                    "base": {str(period): list(state) for period, state in frame.base.items()},
                    "pending": [[b.start, b.minute, b.close] for b in frame.pending],
                    "final_through": frame.final_through,
                }
                for timeframe, frame in self._frames.items()
            },
        }

    @classmethod
    def from_state(cls, saved: Mapping[str, Any], **config: Any) -> "IndicatorEngine":
        """The engine to_state() saved; `config` must match what it was built with, else StateMismatch."""
        engine = cls(**config)
        if saved.get("version") != STATE_VERSION or saved.get("config") != engine._config():
            raise StateMismatch(
                f"saved indicator state is version {saved.get('version')} with {saved.get('config')}; "
                f"this engine is version {STATE_VERSION} with {engine._config()}. Delete the query's "
                "checkpoint to rebuild it (see jobs/indicators.py)."
            )
        engine._newest_minute = saved["newest_minute"]
        for timeframe, frame in engine._frames.items():
            stored = saved["frames"][timeframe]
            frame.base = {int(period): (s[0], s[1], s[2]) for period, s in stored["base"].items()}
            frame.pending = [_Bucket(start, minute, close) for start, minute, close in stored["pending"]]
            frame.final_through = stored["final_through"]
        return engine

    def _config(self) -> Dict[str, Any]:
        return {
            "lengths": list(self._lengths),
            "timeframes": self._timeframe_seconds,
            "settle_minutes": self._settle_minutes,
            "final_only": self._final_only,
        }

    def _apply(self, frame: _Timeframe, minute: int, close: str) -> bool:
        """Put the candle in its bucket; False if that bar is already final."""
        start = minute - minute % frame.seconds
        if frame.final_through is not None and start <= frame.final_through:
            return False
        for i, bucket in enumerate(frame.pending):
            if bucket.start == start:
                # An earlier minute of the bar doesn't move its close.
                if minute > bucket.minute or (minute == bucket.minute and close != bucket.close):
                    bucket.minute, bucket.close = minute, close
                    frame.mark_dirty(start)
                return True
            if bucket.start > start:
                frame.pending.insert(i, _Bucket(start, minute, close))
                frame.mark_dirty(start)
                return True
        frame.pending.append(_Bucket(start, minute, close))
        frame.mark_dirty(start)
        return True

    def _finalize(self, timeframe: str, frame: _Timeframe) -> None:
        """Fold every bar that can no longer change into the running state, emitting it if it changed."""
        settle_seconds = self._settle_minutes * MINUTE_SECONDS
        while frame.pending:
            bucket = frame.pending[0]
            last_minute = bucket.start + frame.seconds - MINUTE_SECONDS
            if self._newest_minute < last_minute + settle_seconds:
                return
            frame.pending.pop(0)
            for period in self._lengths:
                frame.base[period] = ema_step(frame.base[period], float(bucket.close), period)
            if self._final_only or (frame.dirty_from is not None and bucket.start >= frame.dirty_from):
                self._out.append(IndicatorRow(timeframe, bucket.start, {p: s[2] for p, s in frame.base.items()}))
            frame.final_through = bucket.start
