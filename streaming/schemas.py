"""
schemas.py

Spark StructTypes describing the JSON wire format on DSView's Kafka
topics: trades (market.trades.raw) and candles (market.candles.1m).

Deliberately typed as all-StringType here, mirroring the same reasoning
whale_detector.py documents: once an event is on the wire it's just JSON,
and a Spark job shouldn't import ingestion/schema.py's pydantic models to
read it. Casting to real types (Decimal/timestamp) is each job's own
concern, done right after from_json() — keeping this module a pure
description of the wire shape, not a place business types leak into.

Author: @DS
"""

from __future__ import annotations

from pyspark.sql.types import StructField, StructType, StringType

# Matches TradeEvent.model_dump(mode="json") from ingestion/schema.py —
# see producer.py for the exact serialization (Decimal -> str, datetime ->
# ISO 8601 string, enums -> their plain string value).
TRADE_EVENT_SCHEMA = StructType(
    [
        StructField("exchange", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("event_time", StringType(), nullable=False),
        StructField("ingested_at", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("trade_id", StringType(), nullable=False),
        StructField("price", StringType(), nullable=False),
        StructField("quantity", StringType(), nullable=False),
        StructField("side", StringType(), nullable=False),
    ]
)

# market.candles.1m's value, as streaming/candle_topic.py's publish_candles()
# writes it: decimals as strings, times as ISO 8601 strings in UTC, and
# trade_count a JSON number (a StringType field takes its text as-is).
CANDLE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", StringType(), nullable=False),
        StructField("window_end", StringType(), nullable=False),
        StructField("open", StringType(), nullable=False),
        StructField("high", StringType(), nullable=False),
        StructField("low", StringType(), nullable=False),
        StructField("close", StringType(), nullable=False),
        StructField("volume", StringType(), nullable=False),
        StructField("trade_count", StringType(), nullable=False),
    ]
)
