"""
Hand-made bars and a scripted strategy for the backtest tests: every
expected number is then worked out in the test itself.
"""

from __future__ import annotations

from decimal import Decimal

from bar_series import Bar
from strategy import LONG, SHORT, Strategy

HOUR = 3600
T0 = 1_780_000_000 // HOUR * HOUR


def make_bars(prices):
    """One 1h bar per (open, close), high and low around them."""
    bars = []
    for i, (open_, close) in enumerate(prices):
        o, c = Decimal(open_), Decimal(close)
        bars.append(Bar(T0 + i * HOUR, o, max(o, c) + 1, min(o, c) - 1, c, Decimal("1.5"), ema={}))
    return bars


class Scripted(Strategy):
    """Asks for script[i] on the i-th bar it sees ("long", "short", "flat"; nothing for bars not in it)."""

    name = "scripted"

    def __init__(self, script):
        super().__init__()
        self.script = script
        self.seen = []

    def on_bar(self, bar, orders):
        self.seen.append((bar.time, orders.position))
        wanted = self.script.get(len(self.seen) - 1)
        if wanted == LONG:
            orders.go_long()
        elif wanted == SHORT:
            orders.go_short()
        elif wanted == "flat":
            orders.go_flat()


PRICES = [("100", "101"), ("102", "104"), ("105", "103"), ("110", "108"), ("107", "109")]
