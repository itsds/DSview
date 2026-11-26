"""
backtester.py

Steps a strategy through a timeframe's bars and keeps the account
(Phase 4b): the strategy decides on each bar's close (strategy.py), the
broker fills at the next bar's open (broker.py), and the equity is marked
at every bar's close, which is the curve the results panel draws and max
drawdown is measured on (backtest_stats.py).

For each bar from the run's start, oldest first:

1. Fill what the strategy asked for on the bar before, at this bar's open:
   close the position held if it differs from the one asked for, then
   open the new one (a reversal is two fills at the same price, two
   trades in the list).
2. Hand the closed bar to `on_bar()`, with the position as it now stands.
3. Mark the equity at the bar's close.

Bars before the start are not shown to the strategy (their EMAs are
already in every bar's values: bar_series.py warms up from the oldest
stored minute). What it asks for on the last bar has no next open to fill
at and is dropped; a position still open there is closed at the last
bar's close, its trade marked `closed_by_end`, and the curve's last point
is the equity after that exit's fee, so the curve ends at the net profit.

Pure and deterministic: no I/O, no clock, so the same bars and strategy
give the same trades and curve, digit for digit (the kickoff's "a rerun
gives the same result").

Author: @DS
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Sequence, Tuple

from bar_series import Bar
from broker import DEFAULT_CAPITAL, DEFAULT_FEE_RATE, Broker, Trade
from strategy import Orders, Strategy

EquityPoint = Tuple[int, Decimal]  # (bar start, Unix seconds; equity at its close)


@dataclass(frozen=True)
class BacktestResult:
    bars: Sequence[Bar]  # the bars the strategy saw, oldest first
    trades: List[Trade]
    equity: List[EquityPoint]
    capital: Decimal
    fee_rate: Decimal


def run(
    strategy: Strategy,
    bars: Sequence[Bar],
    start: Optional[int] = None,
    capital: Decimal = DEFAULT_CAPITAL,
    fee_rate: Decimal = DEFAULT_FEE_RATE,
) -> BacktestResult:
    """Backtest `strategy` over `bars` from the first bar starting at or after `start` (Unix seconds)."""
    traded = [bar for bar in bars if start is None or bar.time >= start]
    broker = Broker(capital, fee_rate)
    equity: List[EquityPoint] = []
    target: Optional[str] = None
    for index, bar in enumerate(traded):
        if target != broker.position:
            if broker.position is not None:
                broker.close(bar.time, bar.open, index)
            if target is not None and broker.can_open():
                broker.open(target, bar.time, bar.open, index)
        orders = Orders(broker.position)
        strategy.on_bar(bar, orders)
        target = orders.target
        equity.append((bar.time, broker.equity(bar.close)))
    if traded and broker.position is not None:
        last = traded[-1]
        broker.close(last.time, last.close, len(traded), by_end=True)
        equity[-1] = (last.time, broker.cash)
    return BacktestResult(bars=traded, trades=broker.trades, equity=equity, capital=broker.capital, fee_rate=broker.fee_rate)
