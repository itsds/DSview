"""Tests for backtest/strategies/ema_cross.py and the strategy registry, with EMAs set by hand."""

from __future__ import annotations

from decimal import Decimal

import pytest

from bar_series import Bar
from strategies import STRATEGIES, strategy_class
from strategies.ema_cross import EmaCross
from strategy import LONG, SHORT, Orders


def bar(fast, slow, time=0):
    one = Decimal(1)
    return Bar(time, one, one, one, one, one, ema={20: fast, 50: slow})


def decide(strategy, fast, slow, position=None):
    orders = Orders(position)
    strategy.on_bar(bar(fast, slow), orders)
    return orders.target


def test_nothing_happens_while_an_ema_is_still_seeding():
    strategy = EmaCross()
    assert decide(strategy, None, None) is None
    assert decide(strategy, 101.0, None) is None


def test_the_first_bar_with_both_emas_is_no_cross():
    assert decide(EmaCross(), 101.0, 100.0) is None


def test_a_cross_up_goes_long_and_a_cross_down_goes_short():
    strategy = EmaCross()
    decide(strategy, 99.0, 100.0)
    assert decide(strategy, 101.0, 100.0) == LONG
    assert decide(strategy, 102.0, 100.0, position=LONG) == LONG  # still above: hold
    assert decide(strategy, 99.0, 100.0, position=LONG) == SHORT


def test_equal_emas_are_no_cross():
    strategy = EmaCross()
    decide(strategy, 99.0, 100.0)
    assert decide(strategy, 100.0, 100.0) is None
    assert decide(strategy, 99.5, 100.0) is None  # back below: never crossed
    assert decide(strategy, 100.5, 100.0) == LONG


def test_parameters():
    assert EmaCross().ema_lengths == (20, 50)
    assert EmaCross(fast=9, slow=21).ema_lengths == (9, 21)
    assert EmaCross.parse_params({"fast": "10"}) == {"fast": 10}
    with pytest.raises(ValueError):
        EmaCross(fast=50, slow=20)
    with pytest.raises(ValueError):
        EmaCross(period=3)
    with pytest.raises(ValueError):
        EmaCross.parse_params({"speed": "1"})


def test_the_registry_finds_it_by_name():
    assert STRATEGIES["ema_cross"] is EmaCross is strategy_class("ema_cross")
    assert EmaCross.default_timeframe == "1h"
    with pytest.raises(ValueError):
        strategy_class("nope")
