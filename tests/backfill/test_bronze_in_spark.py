"""
Spark reads what the backfill writes to Bronze: both datasets discovered
with their symbol= and date= directories as columns, in the types the live
Bronze uses, and candles built from the aggTrades by the pipeline's own
aggregation (streaming/candle_windowing.py) agree with the klines.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

from binance_rest import BinanceHistoryClient, WeightBudget
from candle_store import CandleStore
from candle_windowing import build_candles
from fake_binance import MINUTE_MS
from history_backfill import DAY_MS, HistoryBackfill

pytestmark = pytest.mark.spark

DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
END = DAY + DAY_MS + 45 * MINUTE_MS


async def test_spark_reads_both_bronze_datasets_and_their_candles_agree(
    spark, fake_binance, fake_time, postgres_schema, symbol, tmp_path
):
    store = CandleStore()
    budget = WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep)
    try:
        async with BinanceHistoryClient(fake_binance.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock) as client:
            await HistoryBackfill(client, store, data_root=str(tmp_path)).run([symbol], 1, 1, END)
    finally:
        store.close()

    trades = spark.read.parquet(f"{tmp_path}/bronze/agg_trades").where(F.col("symbol") == symbol)
    klines = spark.read.parquet(f"{tmp_path}/bronze/klines_1m").where(F.col("symbol") == symbol)

    assert trades.count() == len(list(fake_binance.market.trades_between(symbol, DAY, END)))
    assert {row["date"].isoformat() for row in trades.select("date").distinct().collect()} == {"2026-03-02", "2026-03-03"}
    assert trades.schema["price"].dataType == DecimalType(38, 18)
    assert trades.schema["event_time"].dataType == TimestampType()
    assert klines.count() == 1440 + 45

    built = build_candles(trades.select("symbol", "event_time", "price", "quantity")).alias("built")
    joined = built.join(klines.alias("kline"), ["symbol", "window_start"], "full_outer")
    mismatches = joined.where(
        F.col("kline.open").isNull()
        | F.col("built.open").isNull()
        | (F.col("built.open") != F.col("kline.open"))
        | (F.col("built.high") != F.col("kline.high"))
        | (F.col("built.low") != F.col("kline.low"))
        | (F.col("built.close") != F.col("kline.close"))
        | (F.col("built.volume") != F.col("kline.volume"))
    )
    assert joined.count() == 1440 + 45
    assert mismatches.count() == 0
