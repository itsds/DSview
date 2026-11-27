"""
backtest_stats.py

A run's numbers for the results panel (Phase 4b): net profit, win rate,
max drawdown and profit factor, the ones the roadmap names, plus the
handful a strategy tester usually puts beside them. All from the trades
and the marked-to-market equity curve backtester.py returns.

- **Max drawdown**: the deepest fall of the equity curve from its highest
  point so far (the starting capital counts as the first high), in money,
  and separately the deepest as a percentage of the high it fell from (the
  two can be different falls). On the curve marked at every bar's close,
  so a loss inside a trade counts even if the trade ends well.
- **Profit factor**: gross profit over gross loss; None without a losing
  trade (TradingView shows it as n/a).
- **Win rate**: the share of trades with a positive net P&L; a trade that
  breaks even exactly is neither won nor lost.
- **Buy and hold**: what holding from the first bar's open to the last
  bar's close would have made, fees aside, for comparison.

Every amount is a Decimal (broker.py's context), percentages too; a value
that can't exist (no trades: no average) is None. `to_json()` makes them
plain decimal strings, never floats, so what Postgres stores is what was
computed.

Author: @DS
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Optional

from backtester import BacktestResult
from broker import DECIMAL
from strategy import LONG, SHORT

ZERO = Decimal(0)
HUNDRED = Decimal(100)


def compute_stats(result: BacktestResult) -> Dict[str, Any]:
    trades = result.trades
    capital = result.capital
    final = result.equity[-1][1] if result.equity else capital
    pnls = [t.pnl for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = _sum(wins)
    gross_loss = _sum(losses)
    drawdown, drawdown_pct = _max_drawdown(result)
    net = DECIMAL.subtract(final, capital)
    first, last = (result.bars[0], result.bars[-1]) if result.bars else (None, None)
    return {
        "initial_capital": capital,
        "final_equity": final,
        "net_profit": net,
        "net_profit_pct": _pct(net, capital),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": DECIMAL.divide(gross_profit, DECIMAL.minus(gross_loss)) if losses else None,
        "total_trades": len(trades),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "long_trades": sum(1 for t in trades if t.side == LONG),
        "short_trades": sum(1 for t in trades if t.side == SHORT),
        "win_rate_pct": _pct(Decimal(len(wins)), Decimal(len(trades))) if trades else None,
        "avg_trade": _mean(pnls),
        "avg_win": _mean(wins),
        "avg_loss": _mean(losses),
        "largest_win": max(wins) if wins else None,
        "largest_loss": min(losses) if losses else None,
        "max_drawdown": drawdown,
        "max_drawdown_pct": drawdown_pct,
        "total_fees": _sum(t.fees for t in trades),
        "avg_bars_in_trade": _mean([Decimal(t.bars) for t in trades]),
        "buy_and_hold_pct": _pct(DECIMAL.subtract(last.close, first.open), first.open) if first else None,
        "bars": len(result.bars),
    }


def to_json(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Decimals as plain decimal strings ("12.5", never "1.25E+1"); counts stay ints."""
    return {key: _plain(value) if isinstance(value, Decimal) else value for key, value in stats.items()}


def _max_drawdown(result: BacktestResult):
    peak = result.capital
    worst, worst_pct = ZERO, ZERO
    for _, equity in result.equity:
        if equity > peak:
            peak = equity
            continue
        fall = DECIMAL.subtract(peak, equity)
        worst = max(worst, fall)
        worst_pct = max(worst_pct, _pct(fall, peak))
    return worst, worst_pct


def _pct(part: Decimal, whole: Decimal) -> Optional[Decimal]:
    return DECIMAL.multiply(DECIMAL.divide(part, whole), HUNDRED) if whole else None


def _sum(values) -> Decimal:
    total = ZERO
    for value in values:
        total = DECIMAL.add(total, value)
    return total


def _mean(values) -> Optional[Decimal]:
    return DECIMAL.divide(_sum(values), len(values)) if values else None


def _plain(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text
