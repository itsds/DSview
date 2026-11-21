"""
Tests for streaming/jobs/indicators.py: 1m candles on market.candles.1m
in, EMA 20/50/200 per timeframe out in Postgres (candle_indicators) and
Redis, through real Spark state — including the golden test from trades
all the way to Postgres against the browser's math.js.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest
from aiokafka import AIOKafkaProducer

from config.settings import CANDLES_TOPIC, KAFKA_BOOTSTRAP_SERVERS, SERVER_EMA_LENGTHS
from indicator_engine import INDICATOR_COLUMNS, TIMEFRAME_SECONDS
from jobs.indicators import build_indicator_query
from postgres_sink import IndicatorPostgresSink
from redis_sink import IndicatorRedisSink

from helpers import golden_minutes, make_trade, math_js_ema, wait_for

pytestmark = pytest.mark.spark

# A UTC midnight, so every timeframe's buckets start with the series.
BASE = datetime(2026, 11, 20, tzinfo=timezone.utc)


@pytest.fixture
def start_indicator_query(spark, pg, redis_client, symbol, tmp_path):
    """Starts indicators.py's query (default: a fresh checkpoint), past its first batch; stops them all after."""
    redis_sink = IndicatorRedisSink()
    postgres_sink = IndicatorPostgresSink()
    queries = []

    def start(checkpoint=tmp_path / "checkpoint"):
        query = build_indicator_query(spark, redis_sink, postgres_sink, str(checkpoint), verbose=False)
        queries.append(query)
        wait_for(lambda: query.lastProgress is not None, timeout=60, message="indicator query never started")
        return query

    try:
        yield start
    finally:
        for query in queries:
            query.stop()
        postgres_sink.close()
        pg.execute("DELETE FROM candle_indicators WHERE symbol = %s", (symbol,))
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        for timeframe in TIMEFRAME_SECONDS:
            redis_client.delete(f"indicators:{symbol}:{timeframe}")


@pytest.fixture
async def candle_producer(kafka_topics):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


def _iso(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


async def _send_candles(producer, symbol, minutes):
    """Publish 1m candles as candle_aggregator.py would (candle_topic.py's message shape), in order."""
    for minute, close in minutes:
        candle = {
            "symbol": symbol,
            "window_start": _iso(minute),
            "window_end": _iso(minute + 60),
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": "1",
            "trade_count": 1,
        }
        await producer.send(CANDLES_TOPIC, key=symbol.encode(), value=json.dumps(candle).encode())
    await producer.flush()


def _stored(pg, symbol):
    """candle_indicators for `symbol` as {timeframe: {length: {bar start: value}}}."""
    stored = defaultdict(lambda: defaultdict(dict))
    for row in pg.execute("SELECT * FROM candle_indicators WHERE symbol = %s", (symbol,)):
        start = int(row["window_start"].timestamp())
        for period in SERVER_EMA_LENGTHS:
            stored[row["timeframe"]][period][start] = row[f"ema_{period}"]
    return stored


def _wait_until_stored(pg, symbol, expected, timeout=90):
    """Poll until candle_indicators holds exactly `expected` (math_js_ema's shape) for every timeframe."""

    def matches():
        stored = _stored(pg, symbol)
        return all(
            stored[timeframe][period] == expected[timeframe][period]
            for timeframe in TIMEFRAME_SECONDS
            for period in SERVER_EMA_LENGTHS
        )

    try:
        wait_for(matches, timeout=timeout, interval=1)
    except TimeoutError:
        stored = _stored(pg, symbol)
        for timeframe in TIMEFRAME_SECONDS:
            for period in SERVER_EMA_LENGTHS:
                assert stored[timeframe][period] == expected[timeframe][period], (timeframe, period)
        raise


async def test_golden_trades_end_up_as_the_browsers_ema_in_postgres(
    candle_query, start_indicator_query, trade_producer, pg, symbol, local_timezone
):
    """
    Trades -> candle_aggregator.py -> market.candles.1m -> Spark state ->
    candle_indicators: every bar of every timeframe holds exactly what
    math.js computes from the same candles.
    """
    local_timezone("Asia/Kolkata")  # bar times must come out UTC on the IST laptop too
    start_indicator_query()
    minutes = golden_minutes(120, BASE)
    for minute, close in minutes:
        at = datetime.fromtimestamp(minute, timezone.utc)
        # Two trades a minute: the candle forms, then closes at the series' close.
        await trade_producer.send(make_trade(symbol, str(float(close) + 5), "1", event_time=at + timedelta(seconds=10)))
        await trade_producer.send(make_trade(symbol, close, "2", event_time=at + timedelta(seconds=40)))

    expected = math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS)
    _wait_until_stored(pg, symbol, expected)
    # Still seeding is NULL, not NaN or 0.
    assert expected["1m"][200][minutes[-1][0]] is None
    assert _stored(pg, symbol)["1m"][200][minutes[-1][0]] is None


async def test_a_new_symbol_is_warmed_up_from_stored_candles(start_indicator_query, candle_producer, pg, symbol):
    minutes = golden_minutes(130, BASE, seed=11)
    history, live = minutes[:100], minutes[100:]
    for minute, close in history:
        pg.execute(
            """
            INSERT INTO candles_1m (symbol, window_start, window_end, open, high, low, close, volume, trade_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 1)
            """,
            (symbol, _iso(minute), _iso(minute + 60), close, close, close, close),
        )
    start_indicator_query()

    await _send_candles(candle_producer, symbol, live)

    # Every bar, the stored history's included, as if the job had always been running.
    _wait_until_stored(pg, symbol, math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS))


async def test_a_restarted_query_carries_on_from_its_checkpoint(
    start_indicator_query, candle_producer, pg, symbol, tmp_path
):
    minutes = golden_minutes(90, BASE, seed=5)
    checkpoint = tmp_path / "kept"
    first = start_indicator_query(checkpoint)
    await _send_candles(candle_producer, symbol, minutes[:60])
    _wait_until_stored(pg, symbol, math_js_ema(minutes[:60], TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS))
    first.stop()

    # Nothing in candles_1m to warm up from: only the checkpointed state can
    # continue the EMAs, and candles published while stopped aren't skipped.
    await _send_candles(candle_producer, symbol, minutes[60:75])
    start_indicator_query(checkpoint)
    await _send_candles(candle_producer, symbol, minutes[75:])

    _wait_until_stored(pg, symbol, math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS))


async def test_changed_bars_are_published_and_the_newest_cached_in_redis(
    start_indicator_query, candle_producer, redis_client, symbol
):
    start_indicator_query()
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"indicator_updates:{symbol}:5m")
    minutes = golden_minutes(25, BASE)
    await _send_candles(candle_producer, symbol, minutes)

    newest_bar = BASE + timedelta(minutes=20)
    cached = wait_for(
        lambda: (raw := redis_client.get(f"indicators:{symbol}:5m"))
        and (value := json.loads(raw))["window_start"] == newest_bar.isoformat()
        and value,
        timeout=60,
        message="newest 5m bar never cached",
    )
    assert set(cached) == {"symbol", "timeframe", "window_start", "indicators"}
    assert cached["timeframe"] == "5m"
    assert set(cached["indicators"]) == set(INDICATOR_COLUMNS)
    assert redis_client.ttl(f"indicators:{symbol}:5m") > 0

    published = []
    while (message := pubsub.get_message(timeout=1)) is not None:
        if message["type"] == "message":
            published.append(json.loads(message["data"]))
    assert published, "nothing was published for live subscribers"
    assert published[-1] == cached
    # In time order (a bar can repeat as it changes; with no revisions in this
    # test, none goes back to an older one).
    starts = [m["window_start"] for m in published]
    assert starts == sorted(starts)
    pubsub.close()


async def test_a_backfill_rebuilds_the_indicators_without_a_restart(start_indicator_query, candle_producer, pg, symbol):
    """
    History inserted before the oldest stored candle changes every EMA after
    it. Once the backfill bumps the symbol's revision, the running query's
    next batch rebuilds the symbol from candles_1m: every bar ends up as
    math.js computes it over the whole history, old and new.
    """
    from candle_store import CandleStore  # backfill/'s, the bump the real backfill makes

    minutes = golden_minutes(330, BASE, seed=13)
    older, stored, live = minutes[:230], minutes[230:300], minutes[300:]

    def insert(candles):
        with pg.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO candles_1m (symbol, window_start, window_end, open, high, low, close, volume, trade_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 1)
                """,
                [(symbol, _iso(m), _iso(m + 60), c, c, c, c) for m, c in candles],
            )

    async def go_live(candles):
        insert(candles)  # candle_aggregator.py upserts candles_1m before it publishes
        await _send_candles(candle_producer, symbol, candles)

    insert(stored)
    start_indicator_query()
    await go_live(live[:10])
    _wait_until_stored(pg, symbol, math_js_ema(minutes[230:310], TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS))

    insert(older)  # what the backfill writes...
    store = CandleStore()
    try:
        store.bump_revision(symbol)  # ...and how it says so
    finally:
        store.close()
    await go_live(live[10:])

    _wait_until_stored(pg, symbol, math_js_ema(minutes, TIMEFRAME_SECONDS, SERVER_EMA_LENGTHS))
    pg.execute("DELETE FROM candle_revisions WHERE symbol = %s", (symbol,))
