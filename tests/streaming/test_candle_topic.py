"""
Tests for streaming/candle_topic.py's reading side: market.candles.1m
messages parsed back into typed candles. (The publishing side is tested
through candle_aggregator.py, in test_candle_aggregator.py.)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from candle_topic import parse_candles

pytestmark = pytest.mark.spark


def test_messages_parse_into_typed_candles(spark):
    message = {
        "symbol": "BTCUSDT",
        "window_start": "2026-11-20T10:16:00+00:00",
        "window_end": "2026-11-20T10:17:00+00:00",
        "open": "97000.100000000000000000",
        "high": "97010.000000000000000000",
        "low": "96990.500000000000000000",
        "close": "97005.250000000000000000",
        "volume": "2.500000000000000000",
        "trade_count": 12,
    }
    raw = spark.createDataFrame([(json.dumps(message).encode(), 41)], "value binary, offset long")

    (row,) = parse_candles(raw).collect()

    assert row["symbol"] == "BTCUSDT"
    assert row["window_start"].astimezone(timezone.utc) == datetime(2026, 11, 20, 10, 16, tzinfo=timezone.utc)
    assert row["window_end"].astimezone(timezone.utc) == datetime(2026, 11, 20, 10, 17, tzinfo=timezone.utc)
    assert row["close"] == Decimal("97005.25")
    assert row["volume"] == Decimal("2.5")
    assert row["trade_count"] == 12
    assert row["offset"] == 41
