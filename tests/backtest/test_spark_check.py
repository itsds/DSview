"""
Phase 4b's "the EMAs it trades on equal Spark's on every bar", through the
real thing: the backfill's klines in Bronze and in candles_1m, the live
indicator job (streaming/jobs/indicators.py) warming a new symbol up from
candles_1m as it does after a backfill, and then a backtest whose check
finds every bar's EMA 20, 50 and 200 in candle_indicators, bit for bit.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from aiokafka import AIOKafkaProducer

from config.settings import CANDLES_TOPIC, KAFKA_BOOTSTRAP_SERVERS, POSTGRES_DSN

import run_backtest
from candle_store import CandleStore
from indicator_engine import TIMEFRAME_SECONDS
from jobs.indicators import build_indicator_query
from postgres_sink import IndicatorPostgresSink
from redis_sink import IndicatorRedisSink

from bronze_fakes import DAY_MS, write_klines
from helpers import wait_for

pytestmark = pytest.mark.spark

DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
DAYS = 10


@pytest.fixture
def indicator_query(spark, pg, redis_client, symbol, tmp_path):
    redis_sink, postgres_sink = IndicatorRedisSink(), IndicatorPostgresSink()
    query = build_indicator_query(spark, redis_sink, postgres_sink, str(tmp_path / "checkpoint"), verbose=False)
    try:
        wait_for(lambda: query.lastProgress is not None, timeout=60, message="indicator query never started")
        yield query
    finally:
        query.stop()
        postgres_sink.close()
        for table in ("backtest_runs", "candle_indicators", "candles_1m"):
            pg.execute(f"DELETE FROM {table} WHERE symbol = %s", (symbol,))
        for timeframe in TIMEFRAME_SECONDS:
            redis_client.delete(f"indicators:{symbol}:{timeframe}")


async def test_a_backtest_trades_on_sparks_emas_on_every_bar(tmp_path, symbol, pg, indicator_query, capsys):
    lake = tmp_path / "lake"
    store = CandleStore()
    try:
        for day in write_klines(lake, symbol, DAY, DAY + DAYS * DAY_MS):
            store.upsert_candles(symbol, day)
    finally:
        store.close()

    # The next minute, as candle_aggregator.py would publish it: the job sees
    # a new symbol and warms it up from every candle stored before it.
    minute = (DAY + DAYS * DAY_MS) // 1000
    candle = {"symbol": symbol, "window_start": datetime.fromtimestamp(minute, timezone.utc).isoformat(),
              "window_end": datetime.fromtimestamp(minute + 60, timezone.utc).isoformat(),
              "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1", "trade_count": 1}
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(CANDLES_TOPIC, key=symbol.encode(), value=json.dumps(candle).encode())
    finally:
        await producer.stop()
    count = "SELECT count(*) AS n FROM candle_indicators WHERE symbol = %s AND timeframe = '15m'"
    wait_for(lambda: pg.execute(count, (symbol,)).fetchone()["n"] >= DAYS * 96, timeout=120, message="no warm-up")

    for timeframe in ("15m", "1h", "4h"):
        args = ["--strategy", "ema_cross", "--symbol", symbol, "--timeframe", timeframe]
        assert run_backtest.main(args, data_root=str(lake), dsn=POSTGRES_DSN) == 0, capsys.readouterr().out
        check = pg.execute("SELECT spark_check FROM backtest_runs WHERE symbol = %s ORDER BY run_id DESC LIMIT 1",
                           (symbol,)).fetchone()["spark_check"]
        bars = DAYS * 86400 // TIMEFRAME_SECONDS[timeframe]
        assert check == {"bars": bars, "equal": bars, "differ": 0, "missing": 0, "first_difference": None}, timeframe
