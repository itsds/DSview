"""
indicator_check.py

Phase 4b's check that a backtest trades on the chart's EMAs: every bar's
EMA 20, 50 and 200 (config/settings.py's SERVER_EMA_LENGTHS) against the
row Spark wrote for the same symbol, timeframe and bar start into
`candle_indicators`, exactly, bit for bit (both are doubles from the same
engine; a value still seeding is None on one side and NULL on the other).

A bar Spark has no row for is `missing`, not a difference: the indicator
job may not have run since the backfill, or not yet reached the newest
bars. Pure: backtest_store.py reads Spark's rows.

Author: @DS
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence

from config.settings import SERVER_EMA_LENGTHS

from bar_series import Bar

SparkRows = Mapping[int, Mapping[int, Optional[float]]]  # bar start -> {length: value}


def compare_emas(bars: Sequence[Bar], spark: SparkRows) -> Dict[str, Any]:
    """How many of `bars` equal Spark's values, differ, or have no Spark row; the first difference, if any."""
    equal = differ = missing = 0
    first_difference = None
    for bar in bars:
        row = spark.get(bar.time)
        if row is None:
            missing += 1
            continue
        ours = {length: bar.ema[length] for length in SERVER_EMA_LENGTHS}
        theirs = {length: row.get(length) for length in SERVER_EMA_LENGTHS}
        if ours == theirs:
            equal += 1
        else:
            differ += 1
            if first_difference is None:
                first_difference = {"time": bar.time, "backtest": ours, "spark": theirs}
    return {"bars": len(bars), "equal": equal, "differ": differ, "missing": missing, "first_difference": first_difference}
