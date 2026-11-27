"""
broker.py

The backtest's account (Phase 4's kickoff choices, docs/ROADMAP.md): one
position at a time, all of the equity in each trade, a fee on the traded
value of every fill (0.1% per side by default, Binance spot's base taker
fee), no slippage.

**Entering** commits the whole equity E: the position's value N and its
fee f·N together come to E, so N = E / (1 + f), bought (or sold short) at
the fill price for a quantity of N / price. **Exiting** pays f on the exit
value. A long's net P&L is quantity × (exit − entry) less both fees; a
short's is quantity × (entry − exit) less both, the mirror: no borrow
interest or funding, which Binance spot can't charge (it can't short at
all; a margin or futures short would pay one of them).

**Equity** while a position is open is marked to the given price: the
equity before entry, less the entry fee, plus the open P&L (no exit fee
until there is an exit), so a drawdown inside a trade shows in the curve.

Money is Decimal throughout (the prices are the exchange's decimals), in a
fixed 28-digit context, so a rerun computes the same digits whatever the
caller's decimal context.

Author: @DS
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal
from typing import List, Optional

from strategy import LONG, SHORT

DECIMAL = Context(prec=28)
DEFAULT_CAPITAL = Decimal("10000")
DEFAULT_FEE_RATE = Decimal("0.001")


@dataclass(frozen=True)
class Trade:
    """One round trip, entry to exit."""

    number: int  # 1, 2, … in the order they were opened
    side: str  # "long" or "short"
    entry_time: int  # Unix seconds: the start of the bar whose open filled it
    entry_price: Decimal
    exit_time: int
    exit_price: Decimal
    quantity: Decimal
    fees: Decimal  # entry and exit
    pnl: Decimal  # net of fees
    return_pct: Decimal  # pnl as a percentage of the equity the trade committed
    bars: int  # bars held: from the entry bar up to, not including, the exit bar
    closed_by_end: bool  # still open when the data ended: closed at the last bar's close


@dataclass
class _Position:
    side: str
    entry_time: int
    entry_price: Decimal
    entry_bar: int
    quantity: Decimal
    entry_fee: Decimal
    committed: Decimal  # the equity the trade started from


class Broker:
    """Fills, fees and equity; see the module docstring."""

    def __init__(self, capital: Decimal = DEFAULT_CAPITAL, fee_rate: Decimal = DEFAULT_FEE_RATE) -> None:
        if capital <= 0:
            raise ValueError("starting capital must be positive")
        if not 0 <= fee_rate < 1:
            raise ValueError("the fee rate is a fraction of the traded value, from 0 up to 1")
        self.capital = Decimal(capital)
        self.fee_rate = Decimal(fee_rate)
        self.cash = self.capital  # the equity while flat
        self.trades: List[Trade] = []
        self._open: Optional[_Position] = None

    @property
    def position(self) -> Optional[str]:
        return None if self._open is None else self._open.side

    def can_open(self) -> bool:
        """False once a short has lost all the equity: nothing left to commit."""
        return self.cash > 0

    def open(self, side: str, time: int, price: Decimal, bar: int) -> None:
        if self._open is not None:
            raise RuntimeError("a position is already open")
        if side not in (LONG, SHORT):
            raise ValueError(f"side must be long or short, not {side!r}")
        notional = DECIMAL.divide(self.cash, DECIMAL.add(1, self.fee_rate))
        self._open = _Position(
            side=side,
            entry_time=time,
            entry_price=price,
            entry_bar=bar,
            quantity=DECIMAL.divide(notional, price),
            entry_fee=DECIMAL.multiply(notional, self.fee_rate),
            committed=self.cash,
        )

    def close(self, time: int, price: Decimal, bar: int, by_end: bool = False) -> Trade:
        position = self._open
        if position is None:
            raise RuntimeError("no position to close")
        exit_fee = DECIMAL.multiply(DECIMAL.multiply(position.quantity, price), self.fee_rate)
        fees = DECIMAL.add(position.entry_fee, exit_fee)
        pnl = DECIMAL.subtract(self._gross(position, price), fees)
        self.cash = DECIMAL.add(self.cash, pnl)
        trade = Trade(
            number=len(self.trades) + 1,
            side=position.side,
            entry_time=position.entry_time,
            entry_price=position.entry_price,
            exit_time=time,
            exit_price=price,
            quantity=position.quantity,
            fees=fees,
            pnl=pnl,
            return_pct=DECIMAL.multiply(DECIMAL.divide(pnl, position.committed), 100),
            bars=bar - position.entry_bar,
            closed_by_end=by_end,
        )
        self.trades.append(trade)
        self._open = None
        return trade

    def equity(self, price: Decimal) -> Decimal:
        """The account's value with any open position marked at `price` (before an exit fee)."""
        position = self._open
        if position is None:
            return self.cash
        return DECIMAL.add(DECIMAL.subtract(self.cash, position.entry_fee), self._gross(position, price))

    @staticmethod
    def _gross(position: _Position, price: Decimal) -> Decimal:
        move = DECIMAL.subtract(price, position.entry_price)
        if position.side == SHORT:
            move = DECIMAL.minus(move)
        return DECIMAL.multiply(position.quantity, move)
