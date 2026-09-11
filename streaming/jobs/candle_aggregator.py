"""
candle_aggregator.py

OHLCV candle aggregation — feature roadmap milestone toward the
lightweight-charts frontend. Reads `market.trades.raw`, tumbling-windows
each symbol's trades into 1-minute candles, prints each batch to the
console (still useful as a live sanity check), and writes the latest
candle per symbol to Redis via redis_sink.CandleRedisSink so the (not yet
built) FastAPI backend has real-time state to serve.

This is a Spark Structured Streaming job, unlike whale_detector.py:
computing open/high/low/close/volume needs to aggregate many trades
within a time window, which is exactly the stateful, windowed workload
Spark Structured Streaming is for — see whale_detector.py's docstring for
why *that* job deliberately avoids Spark.

Structured Streaming has no built-in Redis sink, so both the console
print and the Redis write happen inside one foreachBatch — that keeps it
a single streaming query against Kafka rather than two queries reading
the same topic twice.

Run from streaming/ (not repo root, not from inside jobs/) so schemas.py
and spark_session.py resolve as bare imports, matching ingestion/'s flat
import convention (see main.py's docstring):

    cd streaming && python -m jobs.candle_aggregator

Author: @DS
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from redis_sink import CandleRedisSink
from schemas import TRADE_EVENT_SCHEMA
from spark_session import get_spark_session

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TRADES_TOPIC = "market.trades.raw"
CANDLE_WINDOW_DURATION = "1 minute"
# Bounds how long the job waits for late trades before finalizing a window.
# Trade events arrive over a live WebSocket with sub-second lag, so this is
# generous on purpose while verifying correctness, not tuned for latency yet.
WATERMARK_DELAY = "10 seconds"

# price/quantity arrive as strings (see schemas.py); this is precision
# enough for BTCUSDT-scale prices/quantities without losing accuracy.
_DECIMAL = DecimalType(38, 18)


def build_candle_query(
    spark, redis_sink: CandleRedisSink
) -> "pyspark.sql.streaming.StreamingQuery":  # noqa: F821
    """Wire up the read -> parse -> window-aggregate -> console+Redis sink pipeline."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TRADES_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    trades = (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .where(F.col("event_type") == "trade")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("price", F.col("price").cast(_DECIMAL))
        .withColumn("quantity", F.col("quantity").cast(_DECIMAL))
    )

    candles = (
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

    def _write_batch(batch_df, batch_id: int) -> None:
        batch_df.show(truncate=False)
        for row in batch_df.collect():
            redis_sink.write_candle(row.asDict())

    return (
        candles.writeStream.outputMode("update")
        .foreachBatch(_write_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )


def main() -> None:
    spark = get_spark_session("dsview-candle-aggregator")
    redis_sink = CandleRedisSink()
    query = build_candle_query(spark, redis_sink)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
