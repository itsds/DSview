"""Tests for streaming/jobs/storage_writer.py: Bronze, Silver and Gold Parquet from real Spark."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from jobs.candle_aggregator import _row_to_candle
from jobs.storage_writer import start_storage_queries

from helpers import make_trade, wait_for

pytestmark = pytest.mark.spark

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


def _rows(spark, path: str, symbol: str):
    try:
        return spark.read.parquet(path).where(F.col("symbol") == symbol).collect()
    except AnalysisException:
        return []  # nothing written yet


async def test_bronze_keeps_everything_silver_dedups_gold_has_finalized_candles(
    spark, trade_producer, symbol, tmp_path
):
    root = str(tmp_path)
    queries = start_storage_queries(spark, data_root=root)
    try:
        wait_for(
            lambda: all(q.lastProgress is not None for q in queries),
            timeout=90,
            message="storage queries never started",
        )
        at = lambda seconds: BASE + timedelta(seconds=seconds)  # noqa: E731
        for seconds, price, quantity, trade_id in [
            (5, "100", "1", "a"),
            (20, "102", "2", "b"),
            (40, "99", "1", "c"),
            # 10:01:15 pushes the watermark (10 s behind) past 10:01:00,
            # which finalizes the 10:00 window. Sent twice, like a retry.
            (75, "103", "1", f"d-{symbol}"),
            (75, "103", "1", f"d-{symbol}"),
        ]:
            await trade_producer.send(make_trade(symbol, price, quantity, event_time=at(seconds), trade_id=f"{trade_id}-{symbol}"))

        gold = wait_for(lambda: _rows(spark, f"{root}/gold/candles_1m", symbol), timeout=90, message="no Gold candle")
        bronze = wait_for(lambda: len(_rows(spark, f"{root}/bronze/trades", symbol)) == 5 or None, timeout=30)
        silver = wait_for(lambda: len(_rows(spark, f"{root}/silver/trades", symbol)) == 4 or None, timeout=30)
    finally:
        for query in queries:
            query.stop()

    assert bronze and silver  # five rows in Bronze, the repeat gone from Silver

    # Append mode: one row per finalized window, and only the 10:00 one is.
    [candle] = [_row_to_candle(row) for row in gold]
    assert candle["window_start"] == BASE
    assert (candle["open"], candle["high"], candle["low"], candle["close"]) == (
        Decimal("100"), Decimal("102"), Decimal("99"), Decimal("99"),
    )
    assert candle["volume"] == Decimal("4")
    assert candle["trade_count"] == 3
