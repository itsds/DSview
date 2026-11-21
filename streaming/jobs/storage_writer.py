"""
storage_writer.py

Bronze/Silver/Gold Parquet writers for the medallion storage layer (see
CLAUDE.md's architecture table). One Spark application running five
independent streaming queries, rather than five separate processes —
each separate process would spin up its own JVM and re-resolve the Kafka
connector JAR, which is wasteful for a laptop already running
candle_aggregator.py, ingestion, and Redpanda alongside this.

- **Bronze**: `market.trades.raw`, written append-only via
  trades_source.read_trades_stream() — the durable replay/backtest
  source (feature roadmap item 4). "Raw" here means unaggregated and
  undeduplicated at the row level, not literally unparsed JSON — reusing
  the shared parser avoids a second hand-rolled parsing path for a
  "more raw" version that ingestion's own pydantic validation already
  makes unnecessary.
- **Silver**: Bronze, deduplicated by trade_id within the watermark
  window. Ingestion's pydantic validation already guarantees well-formed
  data, so Silver's only real job here is dedup + a queryable, compacted
  layout.
- **Gold**: the same tumbling-window OHLCV aggregation
  candle_aggregator.py computes (candle_windowing.build_candles()), but
  in outputMode("append") instead of "update" — append mode only emits a
  window once its watermark has passed, i.e. exactly once per finalized
  candle. That's what makes it safe to write to an append-only Parquet
  sink; "update" mode would append a growing pile of duplicate,
  progressively-refined rows for the same window instead of one final
  row. candle_aggregator.py's update-mode query writes this same
  aggregation to Redis/Postgres for live serving, where seeing a window
  refine in real time — not just its final value — is the whole point;
  that's why Gold can't just read from candle_aggregator.py's output.
- **Gold indicators** (`gold/indicators`, partitioned by symbol and
  timeframe): EMA 20, 50 and 200 on every timeframe, computed by the same
  engine jobs/indicators.py runs live (indicator_state.py), but in
  final-only mode, over Gold's own finalized candles read back as a file
  stream: Gold derived from Gold, each bar written exactly once, when it
  can no longer change. A bar becomes final when its timeframe's last
  minute has been finalized, so a 1D bar lands just after midnight UTC.
  Its state is in this query's checkpoint, like any streaming state: to
  rebuild, delete both `_checkpoints/gold/indicators` and
  `gold/indicators`, and it recomputes from all of Gold's candles.

- **Gold footprint** (`gold/footprint_1m`): the same footprint
  aggregation candle_aggregator.py's live footprint query runs
  (footprint_windowing.build_footprint()), in append mode for the same
  reason as Gold's candles: each finalized minute's price levels written
  once. Any past minute's footprint, or a volume profile over any range,
  can then be rebuilt from Gold without replaying Bronze. Like Gold's
  candles it holds only what the live stream finalized; the backfill's
  footprint history goes to Postgres alone.

Run from streaming/ (see candle_aggregator.py's docstring for why), with
the repo root on PYTHONPATH:

    cd streaming && python -m jobs.storage_writer

Writes under config/settings.py's DATA_ROOT (storage/data/ by default).

Author: @DS
"""

from __future__ import annotations

import os
from typing import List, Sequence

from pyspark.sql.types import StructType

from config.settings import DATA_ROOT

from candle_windowing import build_candles
from footprint_windowing import build_footprint
from indicator_state import compute_indicators
from spark_session import get_spark_session
from trades_source import read_trades_stream

DEDUP_WATERMARK_DELAY = "10 seconds"


def _start_parquet_stream(df, data_root: str, name: str, output_mode: str, partition_by: Sequence[str] = ("symbol",)):
    return (
        df.writeStream.outputMode(output_mode)
        .format("parquet")
        .option("path", f"{data_root}/{name}")
        .option("checkpointLocation", f"{data_root}/_checkpoints/{name}")
        .partitionBy(*partition_by)
        .start()
    )


def _partition_columns_last(schema: StructType, partition_by: Sequence[str]) -> StructType:
    """
    `schema` with the partition columns moved to the end, the order Spark
    itself gives a partitioned file source (data columns, then partition
    columns). Spark 3.5's streaming Parquet source reads a user-given schema
    with a partition column anywhere else as if it weren't: it hands back
    other columns' bytes as that column, which failed batches with
    IllegalArgumentException or crashed the JVM (SIGSEGV in Murmur3 hashing
    the "symbol" of a groupBy).
    """
    fields = [f for f in schema.fields if f.name not in partition_by]
    return StructType(fields + [schema[name] for name in partition_by])


def start_storage_queries(spark, data_root: str = DATA_ROOT) -> List["pyspark.sql.streaming.StreamingQuery"]:  # noqa: F821
    """Start the Bronze, Silver, Gold candle, Gold indicator and Gold footprint queries under `data_root`; returns them in that order."""
    bronze_trades = read_trades_stream(spark)
    bronze_query = _start_parquet_stream(bronze_trades, data_root, "bronze/trades", "append")

    silver_trades = bronze_trades.withWatermark("event_time", DEDUP_WATERMARK_DELAY).dropDuplicates(
        ["trade_id"]
    )
    silver_query = _start_parquet_stream(silver_trades, data_root, "silver/trades", "append")

    gold_candles = build_candles(read_trades_stream(spark))
    gold_query = _start_parquet_stream(gold_candles, data_root, "gold/candles_1m", "append")

    # The file source needs its directory before the first Gold candle is written.
    os.makedirs(f"{data_root}/gold/candles_1m", exist_ok=True)
    final_candles = spark.readStream.schema(_partition_columns_last(gold_candles.schema, ["symbol"])).parquet(
        f"{data_root}/gold/candles_1m"
    )
    gold_indicators = compute_indicators(final_candles, order_by="window_start", final_only=True)
    indicators_query = _start_parquet_stream(
        gold_indicators, data_root, "gold/indicators", "append", partition_by=("symbol", "timeframe")
    )

    gold_footprint = build_footprint(read_trades_stream(spark))
    footprint_query = _start_parquet_stream(gold_footprint, data_root, "gold/footprint_1m", "append")

    return [bronze_query, silver_query, gold_query, indicators_query, footprint_query]


def main() -> None:
    spark = get_spark_session("dsview-storage-writer")
    queries = start_storage_queries(spark)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
