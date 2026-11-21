"""Tests for streaming/candle_windowing.py's OHLCV aggregation, on a static DataFrame."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from candle_windowing import build_candles
from jobs.candle_aggregator import _row_to_candle

pytestmark = pytest.mark.spark

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)

TRADES_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("price", DecimalType(38, 18)),
    StructField("quantity", DecimalType(38, 18)),
])


def _trade(symbol: str, seconds: int, price: str, quantity: str):
    return (symbol, BASE + timedelta(seconds=seconds), Decimal(price), Decimal(quantity))


def _candles(spark, trades):
    rows = build_candles(spark.createDataFrame(trades, TRADES_SCHEMA)).collect()
    return {(c["symbol"], c["window_start"]): c for c in map(_row_to_candle, rows)}


def test_open_and_close_follow_event_time_not_row_order(spark):
    candles = _candles(spark, [
        # Deliberately out of order, the way a micro-batch can hand them over.
        _trade("AAA", 30, "101", "1"),
        _trade("AAA", 5, "100", "2"),
        _trade("AAA", 50, "99", "0.5"),
        _trade("AAA", 20, "105", "1.5"),
    ])

    candle = candles[("AAA", BASE)]
    assert candle["window_end"] == BASE + timedelta(minutes=1)
    assert candle["open"] == Decimal("100")  # the 10:00:05 trade
    assert candle["close"] == Decimal("99")  # the 10:00:50 trade
    assert candle["high"] == Decimal("105")
    assert candle["low"] == Decimal("99")
    assert candle["volume"] == Decimal("5")
    assert candle["trade_count"] == 4


def test_one_candle_per_symbol_per_minute(spark):
    candles = _candles(spark, [
        _trade("AAA", 10, "100", "1"),
        _trade("AAA", 70, "110", "1"),
        _trade("BBB", 15, "5", "3"),
    ])

    assert set(candles) == {
        ("AAA", BASE),
        ("AAA", BASE + timedelta(minutes=1)),
        ("BBB", BASE),
    }
    assert candles[("AAA", BASE + timedelta(minutes=1))]["open"] == Decimal("110")
    assert candles[("BBB", BASE)]["volume"] == Decimal("3")
