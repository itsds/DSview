"""
storage_writer.py

Bronze/Silver/Gold Parquet writers for the medallion storage layer (see
CLAUDE.md's architecture table). One Spark application running three
independent streaming queries, rather than three separate processes —
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

Run from streaming/ (see candle_aggregator.py's docstring for why):

    cd streaming && python -m jobs.storage_writer

Author: @DS
"""

from __future__ import annotations

from candle_windowing import build_candles
from spark_session import get_spark_session
from trades_source import read_trades_stream

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
DATA_ROOT = "../storage/data"
CHECKPOINT_ROOT = f"{DATA_ROOT}/_checkpoints"
DEDUP_WATERMARK_DELAY = "10 seconds"


def _start_parquet_stream(df, name: str, output_mode: str):
    return (
        df.writeStream.outputMode(output_mode)
        .format("parquet")
        .option("path", f"{DATA_ROOT}/{name}")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{name}")
        .partitionBy("symbol")
        .start()
    )


def main() -> None:
    spark = get_spark_session("dsview-storage-writer")

    bronze_trades = read_trades_stream(spark)
    bronze_query = _start_parquet_stream(bronze_trades, "bronze/trades", "append")

    silver_trades = bronze_trades.withWatermark("event_time", DEDUP_WATERMARK_DELAY).dropDuplicates(
        ["trade_id"]
    )
    silver_query = _start_parquet_stream(silver_trades, "silver/trades", "append")

    gold_candles = build_candles(read_trades_stream(spark))
    gold_query = _start_parquet_stream(gold_candles, "gold/candles_1m", "append")

    queries = [bronze_query, silver_query, gold_query]
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
