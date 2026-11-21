"""
candle_topic.py

Both ends of the market.candles.1m topic (config/settings.py's
CANDLES_TOPIC): publish_candles(), which jobs/candle_aggregator.py calls in
its foreachBatch for every 1m candle update, and read_candles_stream(),
which jobs/indicators.py reads them back with. One module owns the
message shape in both directions, as schemas.py does for trades.

Why a topic at all: the indicators keep running state from candle to
candle in applyInPandasWithState, and Spark 3.5 refuses to run that after
a streaming aggregation in the same query (in update mode and in append
mode alike). So the candles leave candle_aggregator.py's query through
Kafka and enter the indicator query as a source of their own.

Each message is keyed by symbol, so a symbol's candles share a partition
and keep their order, and its value is the candle as Redis gets it
(redis_sink.py), so any consumer can treat the two alike:

    {"symbol": "BTCUSDT",
     "window_start": "2026-11-20T10:16:00+00:00", "window_end": "2026-11-20T10:17:00+00:00",
     "open": "97000.100000000000000000", "high": "...", "low": "...", "close": "...",
     "volume": "2.500000000000000000", "trade_count": 12}

Decimals are strings, exact, as ingestion writes trades; times are ISO
8601 in UTC (spark_session.py sets Spark's session time zone to UTC).
Like the Redis key, a window appears again and again while it forms, each
message replacing the last; consumers keep the latest per window.

**Order matters.** indicator_engine.py relies on reading a symbol's
candles in the order candle_aggregator.py produced them: within a batch
oldest window first, and batch after batch. So each batch is written
sorted, from a single task, with one request in flight at a time.

Author: @DS
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import CANDLES_TOPIC, KAFKA_BOOTSTRAP_SERVERS

from schemas import CANDLE_SCHEMA
from trades_source import DECIMAL_TYPE

# Python's isoformat() for a whole-second UTC time: 2026-11-20T10:16:00+00:00.
_ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssxxx"
_DECIMAL_FIELDS = ("open", "high", "low", "close", "volume")


def publish_candles(candles: DataFrame) -> None:
    """Write a micro-batch of candle_windowing.build_candles() rows to market.candles.1m, oldest window first."""
    value = F.to_json(
        F.struct(
            F.col("symbol"),
            F.date_format("window_start", _ISO_UTC).alias("window_start"),
            F.date_format("window_end", _ISO_UTC).alias("window_end"),
            *[F.col(name).cast("string").alias(name) for name in _DECIMAL_FIELDS],
            F.col("trade_count"),
        )
    )
    (
        # orderBy then coalesce(1): one task writes every row, in window order.
        candles.orderBy("window_start")
        .coalesce(1)
        .select(F.col("symbol").alias("key"), value.alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        # A retried request can't overtake the next one.
        .option("kafka.max.in.flight.requests.per.connection", "1")
        .option("topic", CANDLES_TOPIC)
        .save()
    )


def parse_candles(raw: DataFrame) -> DataFrame:
    """
    Kafka rows of market.candles.1m (value, offset) as typed candles, plus
    `offset`: a symbol's candles all share one partition, so ordering by
    offset is ordering by when they were published.
    """
    return (
        raw.select(F.from_json(F.col("value").cast("string"), CANDLE_SCHEMA).alias("candle"), "offset")
        .select("candle.*", "offset")
        .withColumn("window_start", F.to_timestamp("window_start"))
        .withColumn("window_end", F.to_timestamp("window_end"))
        .withColumn("trade_count", F.col("trade_count").cast("int"))
        .select(
            "symbol",
            "window_start",
            "window_end",
            *[F.col(name).cast(DECIMAL_TYPE).alias(name) for name in _DECIMAL_FIELDS],
            "trade_count",
            "offset",
        )
    )


def read_candles_stream(spark: SparkSession) -> DataFrame:
    """
    Stream market.candles.1m from its end (a query restarted from a
    checkpoint resumes where it stopped instead), parsed by parse_candles().
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", CANDLES_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    return parse_candles(raw)
