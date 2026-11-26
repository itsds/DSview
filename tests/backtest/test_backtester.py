"""
Tests for backtest/backtester.py and broker.py: when orders fill, at what
price, what each fill costs, how equity is marked, and that a rerun comes
out the same. Hand-made bars and a scripted strategy, so every expected
number is worked out here.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from backtest_store import result_digest
from backtester import run
from broker import DECIMAL, Broker
from strategy import LONG, SHORT

from backtest_helpers import PRICES, T0, Scripted, make_bars


def test_a_signal_on_a_close_fills_at_the_next_bars_open():
    bars = make_bars(PRICES)
    result = run(Scripted({0: LONG, 2: "flat"}), bars)
    (trade,) = result.trades
    assert (trade.side, trade.entry_time, trade.entry_price) == (LONG, bars[1].time, Decimal("102"))
    assert (trade.exit_time, trade.exit_price) == (bars[3].time, Decimal("110"))
    assert trade.bars == 2 and not trade.closed_by_end


def test_entry_commits_the_whole_equity_and_both_fills_pay_the_fee():
    result = run(Scripted({0: LONG, 2: "flat"}), make_bars(PRICES))
    (trade,) = result.trades
    notional = DECIMAL.divide(Decimal(10000), Decimal("1.001"))
    quantity = DECIMAL.divide(notional, Decimal(102))
    entry_fee = DECIMAL.multiply(notional, Decimal("0.001"))
    exit_fee = DECIMAL.multiply(DECIMAL.multiply(quantity, Decimal(110)), Decimal("0.001"))
    assert trade.quantity == quantity
    assert trade.fees == DECIMAL.add(entry_fee, exit_fee)
    assert trade.pnl == DECIMAL.subtract(DECIMAL.multiply(quantity, Decimal(8)), trade.fees)
    # A long is spot: the cash after it is what selling the quantity brings in, less the fee.
    assert abs(result.equity[-1][1] - quantity * 110 * Decimal("0.999")) < Decimal("1e-20")


def test_a_short_is_the_mirror_of_a_long():
    result = run(Scripted({0: SHORT, 2: "flat"}), make_bars(PRICES))
    (trade,) = result.trades
    assert trade.side == SHORT
    assert trade.pnl == DECIMAL.subtract(DECIMAL.multiply(trade.quantity, Decimal(102 - 110)), trade.fees)
    assert trade.pnl < 0


def test_equity_is_marked_at_every_bars_close_without_an_exit_fee():
    bars = make_bars(PRICES)
    result = run(Scripted({0: LONG}), bars)
    notional = DECIMAL.divide(Decimal(10000), Decimal("1.001"))
    quantity = DECIMAL.divide(notional, Decimal(102))
    entry_fee = DECIMAL.multiply(notional, Decimal("0.001"))
    assert result.equity[0] == (bars[0].time, Decimal(10000))  # flat through the first bar
    for i in (1, 2, 3):
        marked = Decimal(10000) - entry_fee + quantity * (bars[i].close - Decimal(102))
        assert abs(result.equity[i][1] - marked) < Decimal("1e-20"), i


def test_a_reversal_closes_and_opens_at_the_same_open():
    bars = make_bars(PRICES)
    result = run(Scripted({0: LONG, 1: SHORT}), bars)
    first, second = result.trades
    assert (first.side, first.exit_time, first.exit_price) == (LONG, bars[2].time, Decimal("105"))
    assert (second.side, second.entry_time, second.entry_price) == (SHORT, bars[2].time, Decimal("105"))
    assert second.number == 2


def test_a_position_open_when_the_data_ends_closes_at_the_last_close():
    bars = make_bars(PRICES)
    result = run(Scripted({0: LONG}), bars)
    (trade,) = result.trades
    assert trade.closed_by_end
    assert (trade.exit_time, trade.exit_price) == (bars[-1].time, Decimal("109"))
    assert trade.bars == 4  # bars 1 to 4, the last one included
    # The curve ends at the equity after the exit's fee: the net profit.
    assert result.equity[-1] == (bars[-1].time, DECIMAL.add(Decimal(10000), trade.pnl))


def test_an_order_on_the_last_bar_has_no_open_to_fill_at():
    result = run(Scripted({4: LONG}), make_bars(PRICES))
    assert result.trades == []
    assert [equity for _, equity in result.equity] == [Decimal(10000)] * 5


def test_asking_for_the_position_held_does_nothing():
    result = run(Scripted({0: LONG, 1: LONG, 2: LONG}), make_bars(PRICES))
    assert len(result.trades) == 1


def test_bars_before_the_start_are_neither_shown_nor_traded():
    bars = make_bars(PRICES)
    strategy = Scripted({0: LONG})
    result = run(strategy, bars, start=bars[2].time)
    assert [time for time, _ in strategy.seen] == [b.time for b in bars[2:]]
    assert result.trades[0].entry_time == bars[3].time
    assert [b.time for b in result.bars] == [b.time for b in bars[2:]]


def test_the_strategy_sees_the_position_after_the_bars_fills():
    strategy = Scripted({0: LONG, 2: "flat"})
    run(strategy, make_bars(PRICES))
    assert [position for _, position in strategy.seen] == [None, LONG, LONG, None, None]


def test_a_short_that_loses_everything_opens_nothing_more():
    bars = make_bars([("100", "100"), ("100", "150"), ("250", "250"), ("250", "250"), ("250", "250")])
    result = run(Scripted({0: SHORT, 1: "flat", 2: LONG}), bars)
    (trade,) = result.trades  # the long is never opened: no equity left to commit
    assert trade.pnl < -10000


def test_a_rerun_gives_the_same_result_digit_for_digit():
    bars = make_bars(PRICES * 3)
    script = {0: LONG, 3: SHORT, 7: "flat", 9: LONG}
    first, second = run(Scripted(script), bars), run(Scripted(script), bars)
    assert first.trades == second.trades and first.equity == second.equity
    assert result_digest(first) == result_digest(second)
    assert result_digest(run(Scripted({0: LONG, 3: SHORT}), bars)) != result_digest(first)


def test_the_broker_refuses_nonsense():
    with pytest.raises(ValueError):
        Broker(capital=Decimal(0))
    with pytest.raises(ValueError):
        Broker(fee_rate=Decimal(1))
    broker = Broker()
    with pytest.raises(RuntimeError):
        broker.close(T0, Decimal(1), 0)
    broker.open(LONG, T0, Decimal(100), 0)
    with pytest.raises(RuntimeError):
        broker.open(SHORT, T0, Decimal(100), 0)
