"""Tests for backtest/backtest_stats.py: the results panel's numbers, from hand-made runs."""

from __future__ import annotations

from decimal import Decimal

from backtest_stats import compute_stats, to_json
from backtester import BacktestResult, run
from broker import Trade

from backtest_helpers import PRICES, Scripted, make_bars

T0 = 1_780_000_000


def trade(number, pnl, side="long", fees="1", bars=2):
    return Trade(number, side, T0, Decimal(100), T0 + 60, Decimal(101), Decimal(1), Decimal(fees), Decimal(pnl),
                 Decimal(0), bars, False)


def result(trades, equity):
    bars = make_bars([("100", "100")] * len(equity))
    return BacktestResult(bars=bars, trades=trades, equity=[(b.time, Decimal(e)) for b, e in zip(bars, equity)],
                          capital=Decimal(10000), fee_rate=Decimal("0.001"))


def test_profit_win_rate_and_profit_factor():
    stats = compute_stats(result([trade(1, "300"), trade(2, "-100", "short"), trade(3, "0"), trade(4, "100")],
                                 ["10000", "10300", "10200", "10300"]))
    assert stats["net_profit"] == Decimal(300) and stats["net_profit_pct"] == Decimal(3)
    assert (stats["gross_profit"], stats["gross_loss"]) == (Decimal(400), Decimal(-100))
    assert stats["profit_factor"] == Decimal(4)
    assert (stats["total_trades"], stats["winning_trades"], stats["losing_trades"]) == (4, 2, 1)
    assert stats["win_rate_pct"] == Decimal(50)  # breaking even exactly is neither
    assert (stats["long_trades"], stats["short_trades"]) == (3, 1)
    assert (stats["largest_win"], stats["largest_loss"]) == (Decimal(300), Decimal(-100))
    assert stats["avg_trade"] == Decimal(75) and stats["total_fees"] == Decimal(4)


def test_without_a_losing_trade_the_profit_factor_is_none():
    stats = compute_stats(result([trade(1, "50")], ["10000", "10050"]))
    assert stats["profit_factor"] is None and stats["avg_loss"] is None and stats["largest_loss"] is None


def test_without_trades_the_averages_are_none_and_the_profit_zero():
    stats = compute_stats(result([], ["10000", "10000"]))
    assert stats["net_profit"] == 0 and stats["total_trades"] == 0
    assert stats["win_rate_pct"] is None and stats["avg_trade"] is None


def test_max_drawdown_is_the_deepest_fall_from_the_highest_point_so_far():
    # Up to 12,000, down to 9,000 (-3,000, 25%), up to 15,000, down to 11,500 (-3,500, 23.3%).
    stats = compute_stats(result([], ["10000", "12000", "9000", "15000", "11500"]))
    assert stats["max_drawdown"] == Decimal(3500)
    assert stats["max_drawdown_pct"] == Decimal(25)  # the deepest in percent is the other fall


def test_a_drawdown_inside_a_trade_counts_even_if_the_trade_ends_well():
    bars = make_bars([("100", "100"), ("100", "60"), ("60", "130"), ("130", "130")])
    stats = compute_stats(run(Scripted({0: "long", 2: "flat"}), bars, fee_rate=Decimal(0)))
    assert stats["net_profit"] > 0
    assert stats["max_drawdown_pct"] == Decimal(40)


def test_buy_and_hold_runs_from_the_first_open_to_the_last_close():
    stats = compute_stats(run(Scripted({}), make_bars(PRICES)))
    assert stats["buy_and_hold_pct"] == Decimal(9)  # 100 -> 109


def test_json_has_plain_decimal_strings_and_whole_counts():
    data = to_json(compute_stats(result([trade(1, "12.50")], ["10000", "10012.5"])))
    assert data["net_profit"] == "12.5" and data["net_profit_pct"] == "0.125"
    assert data["total_trades"] == 1 and data["profit_factor"] is None
    assert to_json({"tiny": Decimal("1E-7"), "zero": Decimal("0E-18"), "big": Decimal("1.5E+3")}) == {
        "tiny": "0.0000001", "zero": "0", "big": "1500"
    }
