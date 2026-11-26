"""
clock.py

ReplayClock, the replay's own time: it starts at the replay's start, stands
still until play(), then runs `speed` times as fast as the wall clock. At
MAX speed it doesn't run at all: the player moves it to each trade it sends,
as fast as the pipeline takes them.

Pure: the wall clock is injected (`now`, monotonic seconds), so tests drive
it by hand.

Author: @DS
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Callable, Optional, Union

MAX = "max"
Speed = Union[float, str]  # a multiple of real time, or MAX
MAX_MULTIPLE = 1000.0


def parse_speed(value: object) -> Speed:
    """A speed from a command: MAX, or a multiple of real time in (0, MAX_MULTIPLE]."""
    if value == MAX:
        return MAX
    try:
        multiple = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise ValueError(f"speed must be a number or {MAX!r}, not {value!r}") from None
    if not 0 < multiple <= MAX_MULTIPLE:
        raise ValueError(f"speed must be above 0 and at most {MAX_MULTIPLE:g}, not {multiple:g}")
    return multiple


class ReplayClock:
    def __init__(self, start: datetime, speed: Speed, now: Callable[[], float] = time.monotonic) -> None:
        self._now = now
        self._speed = parse_speed(speed)
        self._origin = start  # replay time at _anchor
        self._anchor = now()
        self._playing = False

    @property
    def speed(self) -> Speed:
        return self._speed

    @property
    def playing(self) -> bool:
        return self._playing

    def time(self) -> datetime:
        if not self._playing or self._speed == MAX:
            return self._origin
        return self._origin + timedelta(seconds=(self._now() - self._anchor) * self._speed)

    def play(self) -> None:
        if not self._playing:
            self._rebase()
            self._playing = True

    def pause(self) -> None:
        if self._playing:
            self._rebase()
            self._playing = False

    def set_speed(self, speed: Speed) -> None:
        self._rebase()
        self._speed = parse_speed(speed)

    def advance_to(self, moment: datetime) -> None:
        """Move forward to `moment` (never back): over a gap in the data, or at MAX speed, to the trade just sent."""
        if moment > self.time():
            self._origin, self._anchor = moment, self._now()

    def wall_seconds_until(self, moment: datetime) -> Optional[float]:
        """Wall seconds until the clock reads `moment`: 0 if it already does or runs at MAX, None while paused."""
        if not self._playing:
            return None
        if self._speed == MAX:
            return 0.0
        return max(0.0, (moment - self.time()).total_seconds() / self._speed)

    def _rebase(self) -> None:
        self._origin, self._anchor = self.time(), self._now()
