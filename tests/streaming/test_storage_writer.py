"""Tests for streaming/jobs/storage_writer.py: Bronze, Silver and Gold Parquet (candles and indicators) from real Spark."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from config.settings import SERVER_EMA_LENGTHS
from indicator_engine import TIMEFRAME_SECONDS
from jobs.candle_aggregator import _row_to_candle
from jobs.storage_writer import start_storage_queries

from helpers import golden_minutes, make_trade, math_js_ema, wait_for

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


async def test_gold_indicators_are_the_browsers_ema_of_gold_candles_once_per_final_bar(
    spark, trade_producer, symbol, tmp_path
):
    root = str(tmp_path)
    midnight = datetime(2026, 11, 20, tzinfo=timezone.utc)
    minutes = golden_minutes(60, midnight, seed=3)
    queries = start_storage_queries(spark, data_root=root)
    try:
        wait_for(lambda: all(q.lastProgress is not None for q in queries), timeout=90, message="never started")
        for minute, close in minutes:
            at = datetime.fromtimestamp(minute, timezone.utc) + timedelta(seconds=30)
            await trade_producer.send(make_trade(symbol, close, "1", event_time=at))
        # 01:01:15 moves the watermark past 01:00:00, finalizing every candle above.
        await trade_producer.send(make_trade(symbol, "1", "1", event_time=midnight + timedelta(minutes=61, seconds=15)))

        path = f"{root}/gold/indicators"
        # The newest bar to become final: 00:59 on 1m, and 00:00 on 1h, its last minute being 00:59.
        rows = wait_for(
            lambda: (found := _rows(spark, path, symbol))
            and {(r["timeframe"], r["window_start"].astimezone(timezone.utc)) for r in found} >= {
                ("1m", midnight + timedelta(minutes=59)),
                ("1h", midnight),
            }
            and found,
            timeout=120,
            message="Gold indicators never caught up",
        )
    finally:
        for query in queries:
            query.stop()

    keys = [(r["timeframe"], r["window_start"]) for r in rows]
    assert len(keys) == len(set(keys)), "a bar was written twice"

    expected = math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS)
    last_minute = minutes[-1][0]
    for timeframe, seconds in TIMEFRAME_SECONDS.items():
        final_starts = {s for s in expected[timeframe][20] if s + seconds - 60 <= last_minute}
        written = {int(r["window_start"].timestamp()): r for r in rows if r["timeframe"] == timeframe}
        assert set(written) == final_starts, timeframe  # 4h and 1D aren't final yet: none
        for period in SERVER_EMA_LENGTHS:
            assert {s: r[f"ema_{period}"] for s, r in written.items()} == {
                s: expected[timeframe][period][s] for s in final_starts
            }, (timeframe, period)
