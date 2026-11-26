"""Tests for replay/clock.py: the replay's own time, on a hand-driven wall clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from clock import MAX, ReplayClock, parse_speed

START = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


class Wall:
    def __init__(self) -> None:
        self.seconds = 1000.0

    def __call__(self) -> float:
        return self.seconds


@pytest.fixture
def wall() -> Wall:
    return Wall()


def test_it_stands_still_until_played_then_runs_at_its_speed(wall):
    clock = ReplayClock(START, 10, now=wall)
    wall.seconds += 5
    assert clock.time() == START and clock.wall_seconds_until(START + timedelta(seconds=1)) is None

    clock.play()
    wall.seconds += 3
    assert clock.time() == START + timedelta(seconds=30)
    assert clock.wall_seconds_until(START + timedelta(seconds=50)) == pytest.approx(2.0)


def test_pause_holds_the_time_and_play_carries_on_from_it(wall):
    clock = ReplayClock(START, 2, now=wall)
    clock.play()
    wall.seconds += 10
    clock.pause()
    wall.seconds += 100
    assert clock.time() == START + timedelta(seconds=20)
    clock.play()
    wall.seconds += 1
    assert clock.time() == START + timedelta(seconds=22)


def test_a_new_speed_applies_from_the_current_time(wall):
    clock = ReplayClock(START, 1, now=wall)
    clock.play()
    wall.seconds += 10
    clock.set_speed(60)
    wall.seconds += 1
    assert clock.time() == START + timedelta(seconds=70)


def test_advance_to_only_ever_moves_forward(wall):
    clock = ReplayClock(START, 1, now=wall)
    clock.play()
    clock.advance_to(START + timedelta(hours=1))
    assert clock.time() == START + timedelta(hours=1)
    clock.advance_to(START)
    assert clock.time() == START + timedelta(hours=1)


def test_at_max_speed_the_clock_moves_only_when_told(wall):
    clock = ReplayClock(START, MAX, now=wall)
    clock.play()
    wall.seconds += 60
    assert clock.time() == START
    assert clock.wall_seconds_until(START + timedelta(days=1)) == 0.0
    clock.advance_to(START + timedelta(minutes=5))
    assert clock.time() == START + timedelta(minutes=5)


@pytest.mark.parametrize("value", [0, -1, 1001, "fast", None])
def test_a_speed_outside_its_range_is_refused(value):
    with pytest.raises(ValueError):
        parse_speed(value)


def test_speeds_parse_from_numbers_and_text():
    assert (parse_speed("10"), parse_speed(0.5), parse_speed(MAX)) == (10.0, 0.5, MAX)
