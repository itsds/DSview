"""
trades_source.py

Shared Kafka read + parse step for every Spark Structured Streaming job
that consumes market.trades.raw (candle_aggregator.py and the Bronze/
Silver/Gold writers in jobs/storage_writer.py). Centralized so the Kafka
options and JSON-parsing/casting logic can't drift between jobs.

Author: @DS
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from config.settings import KAFKA_BOOTSTRAP_SERVERS, PIPELINE, TRADES_TOPIC

from schemas import TRADE_EVENT_SCHEMA

# price/quantity arrive as strings (see schemas.py); this is precision
# enough for BTCUSDT-scale prices/quantities without losing accuracy.
DECIMAL_TYPE = DecimalType(38, 18)


def read_trades_stream(spark: SparkSession) -> DataFrame:
    """
    Read this pipeline's trades topic (market.trades.raw, live), parse its
    JSON, and cast to typed columns.

    A query with no checkpoint starts where the pipeline says
    (Pipeline.starting_offsets): live at the end, replay at the start of
    the run.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TRADES_TOPIC)
        .option("startingOffsets", PIPELINE.starting_offsets)
        .load()
    )

    return (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .where(F.col("event_type") == "trade")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("price", F.col("price").cast(DECIMAL_TYPE))
        .withColumn("quantity", F.col("quantity").cast(DECIMAL_TYPE))
    )
