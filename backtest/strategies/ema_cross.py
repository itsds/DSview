"""
ema_cross.py

The sample strategy (Phase 4b's kickoff decision 4): an EMA 20/50
crossover, long and short, always in the market once it has a signal.
When the fast EMA closes above the slow one it goes long (closing any
short), when it closes below it goes short (closing any long); both fill
at the next bar's open. 1h by default; any timeframe with --timeframe, and
any lengths with --param fast=… --param slow=… (only 20, 50 and 200 have
Spark values to check against, but every length is the engine's own EMA).

A cross is the sign of fast − slow changing from one bar to the next, so
the first signal comes once both EMAs have seeded (the slow one needs
`slow` bars) and they cross; on the bars before, it stays flat. Equal
EMAs are no cross: the last side holds until they part the other way.

Author: @DS
"""

from __future__ import annotations

from typing import Optional, Tuple

from bar_series import Bar
from strategy import Orders, Strategy


class EmaCross(Strategy):
    name = "ema_cross"
    defaults = {"fast": 20, "slow": 50}
    default_timeframe = "1h"

    def __init__(self, **params) -> None:
        super().__init__(**params)
        if not 0 < self.params["fast"] < self.params["slow"]:
            raise ValueError("ema_cross needs 0 < fast < slow")
        self._above: Optional[bool] = None  # fast above slow on the last bar with both

    @property
    def ema_lengths(self) -> Tuple[int, ...]:
        return (self.params["fast"], self.params["slow"])

    def on_bar(self, bar: Bar, orders: Orders) -> None:
        fast, slow = bar.ema[self.params["fast"]], bar.ema[self.params["slow"]]
        if fast is None or slow is None or fast == slow:
            return
        above = fast > slow
        if self._above is not None and above != self._above:
            if above:
                orders.go_long()
            else:
                orders.go_short()
        self._above = above
