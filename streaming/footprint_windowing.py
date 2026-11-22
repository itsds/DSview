"""
footprint_windowing.py

The volume footprint's aggregation (Phase 3b): per symbol, 1-minute
window and price level, how much volume traded at that level by buyers
and by sellers. A level is config/settings.py's FOOTPRINT_PRICE_STEP wide
and named by its lower bound (floor(price / step) * step: with the $1
step, a trade at 97,000.40 is on the 97,000 row); buy and sell are the
taker's side, as ingestion normalizes every trade.

Shared, like candle_windowing.build_candles(), by the live query in
jobs/candle_aggregator.py (outputMode "update": every level a batch
changed, to Postgres and Redis) and the Gold writer in
jobs/storage_writer.py (outputMode "append": each finalized minute's
levels exactly once). It uses candle_windowing's window length and
watermark, not copies of them, so a minute's footprint and its candle are
cut from the same trades: the footprint's buy + sell over a minute's
levels is that minute's candle volume.

backfill/footprint_rows.py does the same bucketing for history in
pyarrow, from the exchange's aggTrades; tests check the two agree.

Author: @DS
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from config.settings import FOOTPRINT_PRICE_STEP

from candle_windowing import CANDLE_WINDOW_DURATION, WATERMARK_DELAY
from trades_source import DECIMAL_TYPE


def price_level(price: Column, step: Decimal = FOOTPRINT_PRICE_STEP) -> Column:
    """The level a price falls on: floor(price / step) * step, at trades' own decimal type."""
    step_lit = F.lit(step).cast(DECIMAL_TYPE)
    return (F.floor(price / step_lit) * step_lit).cast(DECIMAL_TYPE)


def build_footprint(trades: DataFrame) -> DataFrame:
    """
    Footprint rows from a parsed trades DataFrame (trades_source.py):
    symbol, window_start, window_end, price (the level), buy_volume,
    sell_volume. One row per level that traded in a window; a level only
    one side traded at has 0 for the other.
    """
    zero = F.lit(0).cast(DECIMAL_TYPE)
    is_buy = F.col("side") == "buy"
    return (
        trades.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), CANDLE_WINDOW_DURATION).alias("candle_window"),
            price_level(F.col("price")).alias("price"),
        )
        .agg(
            F.sum(F.when(is_buy, F.col("quantity")).otherwise(zero)).cast(DECIMAL_TYPE).alias("buy_volume"),
            F.sum(F.when(is_buy, zero).otherwise(F.col("quantity"))).cast(DECIMAL_TYPE).alias("sell_volume"),
        )
        .select(
            "symbol",
            F.col("candle_window.start").alias("window_start"),
            F.col("candle_window.end").alias("window_end"),
            "price",
            "buy_volume",
            "sell_volume",
        )
    )
