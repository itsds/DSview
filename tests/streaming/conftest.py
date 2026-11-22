"""
Fixtures shared by the streaming tests: the running Spark queries.
"""

from __future__ import annotations

import pytest

from helpers import wait_for


@pytest.fixture
def candle_query(spark, pg, redis_client, symbol):
    """candle_aggregator's query, running and past its first (empty) batch, so offsets are pinned."""
    from jobs.candle_aggregator import build_candle_query
    from postgres_sink import CandlePostgresSink
    from redis_sink import CandleRedisSink

    redis_sink = CandleRedisSink()
    postgres_sink = CandlePostgresSink()
    query = build_candle_query(spark, redis_sink, postgres_sink)
    try:
        wait_for(lambda: query.lastProgress is not None, timeout=60, message="candle query never started")
        yield query
    finally:
        query.stop()
        postgres_sink.close()
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        redis_client.delete(f"candle:{symbol}:1m")


@pytest.fixture
def footprint_query(spark, pg, redis_client, symbol):
    """candle_aggregator's footprint query, running and past its first (empty) batch."""
    from jobs.candle_aggregator import build_footprint_query
    from postgres_sink import FootprintPostgresSink
    from redis_sink import FootprintRedisSink

    postgres_sink = FootprintPostgresSink()
    query = build_footprint_query(spark, FootprintRedisSink(), postgres_sink)
    try:
        wait_for(lambda: query.lastProgress is not None, timeout=60, message="footprint query never started")
        yield query
    finally:
        query.stop()
        postgres_sink.close()
        pg.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))
        for key in redis_client.scan_iter(f"footprint*:{symbol}:*"):
            redis_client.delete(key)
