"""
candle_windowing.py

Shared tumbling-window OHLCV aggregation, used by both
jobs/candle_aggregator.py (live serving, outputMode "update") and
jobs/storage_writer.py's Gold writer (durable Parquet, outputMode
"append") — same aggregation logic, different output modes for different
purposes; see storage_writer.py's module docstring for why those can't
share one query.

min_by/max_by (not first()/last()) compute open/close: Spark Structured
Streaming gives no ordering guarantee for rows within a group, so open
and close have to be picked out explicitly by event_time rather than
assumed from row order.

Author: @DS
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CANDLE_WINDOW_DURATION = "1 minute"
# Bounds how long a job waits for late trades before finalizing a window.
# Trade events arrive over a live WebSocket with sub-second lag, so this
# is generous on purpose while verifying correctness, not tuned for
# latency yet.
WATERMARK_DELAY = "10 seconds"


def build_candles(trades: DataFrame) -> DataFrame:
    """Tumbling-window OHLCV aggregation per symbol from a parsed trades DataFrame."""
    return (
        trades.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), CANDLE_WINDOW_DURATION).alias("candle_window"),
        )
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("candle_window.start").alias("window_start"),
            F.col("candle_window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        )
    )
