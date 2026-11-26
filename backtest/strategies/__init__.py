"""
strategies/

Every strategy run_backtest.py can run, by the name --strategy takes. A new
strategy is a Strategy subclass (strategy.py) in a module of its own here,
added to STRATEGIES.

Author: @DS
"""

from __future__ import annotations

from typing import Dict, Type

from strategy import Strategy

from strategies.ema_cross import EmaCross

STRATEGIES: Dict[str, Type[Strategy]] = {cls.name: cls for cls in (EmaCross,)}


def strategy_class(name: str) -> Type[Strategy]:
    try:
        return STRATEGIES[name]
    except KeyError:
        raise ValueError(f"no strategy {name!r}; there are {sorted(STRATEGIES)}") from None
