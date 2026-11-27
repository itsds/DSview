"""
strategy.py

What a strategy is (Phase 4's kickoff decision 3): a Python class with
`on_bar()`, stepped by backtester.py through one timeframe's bars, oldest
first. Strategies live in strategies/ and are registered there by name,
which is how run_backtest.py finds them.

    class MyStrategy(Strategy):
        name = "my_strategy"
        defaults = {"length": 20}

        @property
        def ema_lengths(self):
            return (self.params["length"],)

        def on_bar(self, bar, orders):
            if bar.ema[self.params["length"]] is None:
                return  # still seeding
            if float(bar.close) > bar.ema[self.params["length"]]:
                orders.go_long()
            else:
                orders.go_flat()

**What on_bar() sees**: a bar.Bar once it has closed: its OHLCV, the EMA
of every length `ema_lengths` asks for at its close (None while one is
still seeding), and, with `needs_footprint`, its footprint where Bronze's
aggTrades cover it (None elsewhere). Nothing of later bars, so a strategy
can't look ahead.

**What it can do**: say which position it wants, through `orders`: long,
short or flat, one position at a time, all of the capital in it
(the kickoff's choices). The last call in a bar wins, and the fill is at
the next bar's open (broker.py), so a decision on a bar's close is never
filled at a price that bar already showed. Asking for the position it
already holds does nothing; asking for the other side closes the position
and opens the new one at the same open.

Author: @DS
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Mapping, Optional, Tuple

from bar_series import Bar

LONG = "long"
SHORT = "short"
SIDES = (LONG, SHORT)


class Orders:
    """A strategy's handle on its position, for one bar: read it, or ask for another from the next bar's open."""

    def __init__(self, position: Optional[str]) -> None:
        self._position = position
        self.target: Optional[str] = position  # what to hold from the next bar's open

    @property
    def position(self) -> Optional[str]:
        """The position held through this bar: "long", "short" or None (flat)."""
        return self._position

    def go_long(self) -> None:
        self.target = LONG

    def go_short(self) -> None:
        self.target = SHORT

    def go_flat(self) -> None:
        self.target = None


class Strategy:
    """Base class of every strategy; see the module docstring."""

    name: ClassVar[str] = ""  # what run_backtest.py's --strategy calls it
    defaults: ClassVar[Dict[str, Any]] = {}  # every parameter, with its default value and so its type
    default_timeframe: ClassVar[str] = "1h"
    needs_footprint: ClassVar[bool] = False

    def __init__(self, **params: Any) -> None:
        unknown = set(params) - set(self.defaults)
        if unknown:
            raise ValueError(f"{self.name} has no parameter {', '.join(sorted(unknown))}; it takes {sorted(self.defaults)}")
        self.params: Dict[str, Any] = {**self.defaults, **params}

    @property
    def ema_lengths(self) -> Tuple[int, ...]:
        """The EMA lengths on_bar() reads from bar.ema."""
        return ()

    def on_bar(self, bar: Bar, orders: Orders) -> None:
        raise NotImplementedError

    @classmethod
    def parse_params(cls, pairs: Mapping[str, str]) -> Dict[str, Any]:
        """Command-line "name=value" strings as parameters, each of its default's type."""
        params: Dict[str, Any] = {}
        for key, text in pairs.items():
            if key not in cls.defaults:
                raise ValueError(f"{cls.name} has no parameter {key}; it takes {sorted(cls.defaults)}")
            default = cls.defaults[key]
            if isinstance(default, bool):
                if text.lower() not in ("true", "false"):
                    raise ValueError(f"{key} is true or false, not {text!r}")
                params[key] = text.lower() == "true"
            else:
                params[key] = type(default)(text)
        return params
