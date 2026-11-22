"""
Tests for the volume footprint (Phase 3b): streaming/footprint_windowing.py's
aggregation on a static DataFrame, the Postgres and Redis sinks on their
own, and jobs/candle_aggregator.py's live footprint query from trades on
Kafka to footprint_1m and Redis, through real Spark.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from helpers import make_trade, wait_for

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)

TRADES_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("price", DecimalType(38, 18)),
    StructField("quantity", DecimalType(38, 18)),
    StructField("side", StringType()),
])


def _at(seconds: float) -> datetime:
    return BASE + timedelta(seconds=seconds)


def _trade(symbol: str, seconds: float, price: str, quantity: str, side: str = "buy"):
    return (symbol, _at(seconds), Decimal(price), Decimal(quantity), side)


def _levels(spark, trades):
    """build_footprint() over static trades, as {(symbol, window_start, price): (buy, sell)}."""
    from footprint_windowing import build_footprint

    rows = build_footprint(spark.createDataFrame(trades, TRADES_SCHEMA)).collect()
    return {
        (r["symbol"], r["window_start"].astimezone(timezone.utc), r["price"]): (r["buy_volume"], r["sell_volume"])
        for r in rows
    }


# --- the aggregation ----------------------------------------------------------


@pytest.mark.spark
def test_trades_land_on_the_level_below_them_split_by_taker_side(spark):
    levels = _levels(spark, [
        _trade("AAA", 5, "97000.40", "1", "buy"),
        _trade("AAA", 6, "97000.99", "0.5", "sell"),
        _trade("AAA", 7, "97000.00", "0.25", "buy"),  # exactly on a level: that level
        _trade("AAA", 8, "97001.00", "2", "sell"),
        _trade("AAA", 9, "96999.999", "3", "buy"),
        _trade("AAA", 65, "97000.50", "4", "buy"),  # the next minute
        _trade("BBB", 10, "5.5", "7", "sell"),
    ])

    assert levels == {
        ("AAA", BASE, Decimal("96999")): (Decimal("3"), Decimal("0")),
        ("AAA", BASE, Decimal("97000")): (Decimal("1.25"), Decimal("0.5")),
        ("AAA", BASE, Decimal("97001")): (Decimal("0"), Decimal("2")),
        ("AAA", BASE + timedelta(minutes=1), Decimal("97000")): (Decimal("4"), Decimal("0")),
        ("BBB", BASE, Decimal("5")): (Decimal("0"), Decimal("7")),
    }


@pytest.mark.spark
def test_a_minutes_levels_add_up_to_its_candle_volume_exactly(spark):
    from candle_windowing import build_candles

    rng = random.Random(7)
    trades = [
        _trade("AAA", rng.uniform(0, 300), f"{97000 + rng.uniform(-40, 40):.2f}", f"{rng.uniform(0, 2):.6f}", rng.choice(["buy", "sell"]))
        for _ in range(2000)
    ]
    levels = _levels(spark, trades)
    candles = build_candles(spark.createDataFrame(trades, TRADES_SCHEMA)).collect()

    assert len(candles) == 5
    for candle in candles:
        start = candle["window_start"].astimezone(timezone.utc)
        total = sum(buy + sell for (_, window, _), (buy, sell) in levels.items() if window == start)
        assert total == candle["volume"]
        # Every level between the candle's low and high, and none outside.
        prices = sorted(price for (_, window, price) in levels if window == start)
        assert prices[0] == int(candle["low"]) and prices[-1] == int(candle["high"])


@pytest.mark.spark
def test_price_level_honours_a_coarser_step(spark):
    from pyspark.sql import functions as F

    from footprint_windowing import price_level

    df = spark.createDataFrame([(Decimal(p),) for p in ("97003.5", "97005", "97009.99", "96999.1")], "price decimal(38,18)")
    got = [r["level"] for r in df.select(price_level(F.col("price"), Decimal("5")).alias("level")).collect()]
    assert got == [Decimal("97000"), Decimal("97005"), Decimal("97005"), Decimal("96995")]


# --- the sinks ----------------------------------------------------------------


def _row(symbol, window_start, price, buy, sell):
    return {
        "symbol": symbol,
        "window_start": window_start,
        "price": Decimal(price),
        "buy_volume": Decimal(buy),
        "sell_volume": Decimal(sell),
    }


def _drain(pubsub):
    messages = []
    while (message := pubsub.get_message(timeout=0.5)) is not None:
        if message["type"] == "message":
            messages.append(json.loads(message["data"]))
    return messages


def test_redis_sink_publishes_each_touched_minute_whole(redis_client, symbol):
    from redis_sink import FootprintRedisSink

    sink = FootprintRedisSink()
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"footprint_updates:{symbol}:1m")
    pubsub.get_message(timeout=1)  # the subscribe confirmation

    sink.write_rows([
        _row(symbol, BASE, "97001.000000000000000000", "0.500000000000000000", "0"),
        _row(symbol, BASE, "97000", "1.25", "2"),
    ])
    # A later batch changes one level: the message still carries all of them.
    sink.write_rows([_row(symbol, BASE, "97001", "0.75", "0.1")])

    first, second = _drain(pubsub)
    assert first == {
        "symbol": symbol,
        "window_start": "2026-11-20T10:00:00+00:00",
        "levels": [["97000", "1.25", "2"], ["97001", "0.5", "0"]],
    }
    assert second["levels"] == [["97000", "1.25", "2"], ["97001", "0.75", "0.1"]]
    assert json.loads(redis_client.get(f"footprint:{symbol}:1m")) == second
    assert 0 < redis_client.ttl(f"footprint:{symbol}:1m") <= 120
    levels_key = f"footprint_levels:{symbol}:1m:{int(BASE.timestamp())}"
    assert 0 < redis_client.ttl(levels_key) <= 180
    pubsub.close()


def test_redis_sink_keeps_the_newest_minute_cached_through_a_late_revision(redis_client, symbol):
    from redis_sink import FootprintRedisSink

    sink = FootprintRedisSink()
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"footprint_updates:{symbol}:1m")
    pubsub.get_message(timeout=1)

    later = BASE + timedelta(minutes=1)
    # Two minutes in one batch, newest listed first: still published oldest first.
    sink.write_rows([_row(symbol, later, "97010", "1", "0"), _row(symbol, BASE, "97000", "1", "0")])
    sink.write_rows([_row(symbol, BASE, "97000", "2", "0")])  # a late trade revises the older minute

    starts = [m["window_start"] for m in _drain(pubsub)]
    assert starts == ["2026-11-20T10:00:00+00:00", "2026-11-20T10:01:00+00:00", "2026-11-20T10:00:00+00:00"]
    assert json.loads(redis_client.get(f"footprint:{symbol}:1m"))["window_start"] == "2026-11-20T10:01:00+00:00"
    pubsub.close()
    for key in redis_client.scan_iter(f"footprint*:{symbol}:*"):
        redis_client.delete(key)


def test_postgres_sink_upserts_running_totals(pg, symbol):
    from postgres_sink import FootprintPostgresSink

    sink = FootprintPostgresSink()
    try:
        sink.write_rows([_row(symbol, BASE, "97000", "1", "0"), _row(symbol, BASE, "97001", "0", "2")])
        sink.write_rows([_row(symbol, BASE, "97000", "1.5", "0.25")])
        sink.write_rows([])
    finally:
        sink.close()

    rows = pg.execute(
        "SELECT price, buy_volume, sell_volume FROM footprint_1m WHERE symbol = %s ORDER BY price", (symbol,)
    ).fetchall()
    assert [tuple(r.values()) for r in rows] == [
        (Decimal("97000"), Decimal("1.5"), Decimal("0.25")),
        (Decimal("97001"), Decimal("0"), Decimal("2")),
    ]
    pg.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))


# --- the live query -----------------------------------------------------------


@pytest.mark.spark
async def test_live_trades_become_footprint_rows_and_live_grids(
    footprint_query, trade_producer, pg, redis_client, symbol, local_timezone
):
    local_timezone("Asia/Kolkata")  # collect()'s naive local times must still land as UTC
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"footprint_updates:{symbol}:1m")

    for seconds, price, quantity, side in [
        (5, "97000.40", "1", "buy"),
        (20, "97000.90", "0.5", "sell"),
        (40, "97002.10", "2", "buy"),
        (55, "97002.00", "0.25", "sell"),
    ]:
        await trade_producer.send(make_trade(symbol, price, quantity, side=side, event_time=_at(seconds)))

    rows = wait_for(
        lambda: (
            rows := pg.execute(
                "SELECT window_start, price, buy_volume, sell_volume FROM footprint_1m WHERE symbol = %s ORDER BY price",
                (symbol,),
            ).fetchall()
        )
        and sum(r["buy_volume"] + r["sell_volume"] for r in rows) == Decimal("3.75")
        and rows,
        timeout=60,
        message="footprint never reached Postgres",
    )
    assert [(r["window_start"], r["price"], r["buy_volume"], r["sell_volume"]) for r in rows] == [
        (BASE, Decimal("97000"), Decimal("1"), Decimal("0.5")),
        (BASE, Decimal("97002"), Decimal("2"), Decimal("0.25")),
    ]

    cached = json.loads(redis_client.get(f"footprint:{symbol}:1m"))
    assert cached == {
        "symbol": symbol,
        "window_start": "2026-11-20T10:00:00+00:00",
        "levels": [["97000", "1", "0.5"], ["97002", "2", "0.25"]],
    }
    published = _drain(pubsub)
    assert published and published[-1] == cached
    pubsub.close()


@pytest.mark.spark
async def test_live_footprint_volume_equals_live_candle_volume(
    candle_query, footprint_query, trade_producer, pg, symbol
):
    rng = random.Random(11)
    trades = sorted(
        (rng.uniform(0, 179), f"{97000 + rng.uniform(-25, 25):.2f}", f"{rng.uniform(0.001, 1.5):.5f}", rng.choice(["buy", "sell"]))
        for _ in range(300)
    )
    for seconds, price, quantity, side in trades:
        await trade_producer.send(make_trade(symbol, price, quantity, side=side, event_time=_at(seconds)))
    expected = sum(Decimal(q) for _, _, q, _ in trades)

    def totals():
        candles = pg.execute(
            "SELECT window_start, volume FROM candles_1m WHERE symbol = %s ORDER BY window_start", (symbol,)
        ).fetchall()
        levels = pg.execute(
            """
            SELECT window_start, sum(buy_volume + sell_volume) AS volume
            FROM footprint_1m WHERE symbol = %s GROUP BY window_start ORDER BY window_start
            """,
            (symbol,),
        ).fetchall()
        done = sum(c["volume"] for c in candles) == expected == sum(l["volume"] for l in levels)
        return done and (candles, levels)

    candles, levels = wait_for(totals, timeout=90, message="candles and footprint never both caught up")
    assert len(candles) == 3
    assert [(c["window_start"], c["volume"]) for c in candles] == [(l["window_start"], l["volume"]) for l in levels]
