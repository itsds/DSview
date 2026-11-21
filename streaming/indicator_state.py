"""
indicator_state.py

The Spark side of the indicators: compute_indicators() runs
indicator_engine.py's IndicatorEngine inside applyInPandasWithState, one
engine per symbol, with the engine's state kept by Spark between
micro-batches (and in the query's checkpoint, so a restarted query carries
on where it stopped). Used by jobs/indicators.py (live, from the candle
topic) and jobs/storage_writer.py (Gold, from Gold's final candles).

Why arbitrary state rather than a window aggregation: an EMA bar depends
on every bar before it, not on the trades of one window, so each symbol's
running EMAs have to be carried from batch to batch. applyInPandasWithState
is Spark 3.5's Python API for that (the pandas form of Scala's
flatMapGroupsWithState): Spark groups each micro-batch by symbol, calls the
function with that symbol's new rows and its saved state, and stores
whatever state it leaves. The state is the engine's to_state() as one JSON
string, which keeps Spark's state schema fixed however the engine's
evolves (a layout change is refused by STATE_VERSION, not misread).

**Warm-up.** The first time a live query sees a symbol it has no state,
so the engine is first fed every 1m candle stored in Postgres before that
batch (`warm_up`; postgres_sink.read_minute_closes()), and the rows for
that whole history come out with the batch. Charts therefore get server
values for all the history they can scroll to, not just from the day the
job first ran. The checkpoint is what makes this happen only once; delete
it to rebuild every symbol's indicators from candles_1m.

The function runs in Spark's Python worker processes, which import this
module and indicator_engine.py by name: jobs run from streaming/, which
puts it on their path (the tests add it to PYTHONPATH).

Author: @DS
"""

from __future__ import annotations

import json
import logging
from typing import Callable, Iterable, Iterator, Optional, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from config.settings import SERVER_EMA_LENGTHS

from indicator_engine import INDICATOR_COLUMNS, LIVE_SETTLE_MINUTES, IndicatorEngine

logger = logging.getLogger(__name__)

_OUTPUT_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("timeframe", StringType()),
        StructField("window_start_s", LongType()),
        *[StructField(column, DoubleType()) for column in INDICATOR_COLUMNS],
    ]
)
_STATE_SCHEMA = StructType([StructField("engine", StringType())])

WarmUp = Callable[[str, int], Iterable[Tuple[int, str]]]


def compute_indicators(
    candles: DataFrame, *, order_by: str, final_only: bool = False, warm_up: Optional[WarmUp] = None
) -> DataFrame:
    """
    Indicator rows (symbol, timeframe, window_start, ema_20, ...) from a
    stream of 1m candles with at least symbol, window_start, close and the
    `order_by` column, the order the engine must see them in (the candle
    topic's offset; for final candles, window_start).

    Live (final_only=False): a row for every bar whose values may have
    changed in a batch, provisional bars included; the query's output mode
    must be "update". final_only=True: each bar once, when it can no longer
    change, for an append-mode query; the input must then be final candles.
    `warm_up(symbol, before)` supplies the history a symbol's engine is
    seeded with when it has no state yet: (Unix seconds, close) of the 1m
    candles before `before`, oldest first.
    """
    settle_minutes = 0 if final_only else LIVE_SETTLE_MINUTES

    def update_symbol(
        key: Tuple[str], batches: Iterator[pd.DataFrame], state: GroupState
    ) -> Iterator[pd.DataFrame]:
        (symbol,) = key
        batch = pd.concat(list(batches), ignore_index=True).sort_values("seq", kind="stable")
        if state.exists:
            engine = IndicatorEngine.from_state(
                json.loads(state.get[0]), settle_minutes=settle_minutes, final_only=final_only
            )
        else:
            engine = IndicatorEngine(settle_minutes=settle_minutes, final_only=final_only)
            if warm_up is not None:
                for minute, close in warm_up(symbol, int(batch["minute"].min())):
                    engine.update(minute, close)

        for minute, close in zip(batch["minute"].tolist(), batch["close"].tolist()):
            engine.update(int(minute), close)
        rows = engine.drain()
        if engine.ignored:
            logger.warning("%s: %d candle updates arrived after their bar was final; ignored", symbol, engine.ignored)
        state.update((json.dumps(engine.to_state()),))

        if rows:
            yield pd.DataFrame(
                {
                    "symbol": symbol,
                    "timeframe": [row.timeframe for row in rows],
                    "window_start_s": [row.window_start for row in rows],
                    # None while seeding: Spark turns pandas' NaN into a SQL NULL.
                    **{
                        f"ema_{period}": pd.Series([row.ema[period] for row in rows], dtype="float64")
                        for period in SERVER_EMA_LENGTHS
                    },
                },
                columns=[field.name for field in _OUTPUT_SCHEMA.fields],
            )

    inputs = candles.select(
        "symbol",
        F.col("window_start").cast("long").alias("minute"),
        F.col("close").cast("string").alias("close"),
        F.col(order_by).cast("long").alias("seq"),
    )
    output = inputs.groupBy("symbol").applyInPandasWithState(
        update_symbol,
        outputStructType=_OUTPUT_SCHEMA,
        stateStructType=_STATE_SCHEMA,
        outputMode="append" if final_only else "update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )
    return output.select(
        "symbol",
        "timeframe",
        F.timestamp_seconds("window_start_s").alias("window_start"),
        *INDICATOR_COLUMNS,
    )
