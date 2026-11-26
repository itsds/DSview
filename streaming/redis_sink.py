"""
redis_sink.py

Redis serving-layer writers for windowed output: CandleRedisSink for
candle_aggregator.py's candles, FootprintRedisSink for its footprint
levels, IndicatorRedisSink for jobs/indicators.py's indicator values.

Structured Streaming has no built-in Redis sink, so candle_aggregator.py
drives this through foreachBatch (see its module docstring for why).
Kept as its own module — rather than inline in candle_aggregator.py — so
the write logic lives in one place for every job that pushes state
through the same Redis instance.

Only the *latest* candle per symbol/timeframe is kept (one key,
overwritten every batch) — Redis here is real-time serving state, not
history. Historical candles are Postgres's job once that layer exists.

Writes go two places, per the "WebSocket for live push from Redis
pub/sub" row of CLAUDE.md's architecture table:
  - a SET with TTL (`candle:{symbol}:{timeframe}`) so a client connecting
    mid-window can fetch current state immediately instead of waiting for
    the next update;
  - a PUBLISH (`candle_updates:{symbol}:{timeframe}`) so already-connected
    clients (backend/routers/ws.py) get pushed each update as it happens.

Every key and channel starts with the pipeline's Redis prefix (none for
live, `replay:` for replay; config/settings.py's Pipeline), so a replay's
jobs never overwrite live's state. The keys named in these docstrings are
live's.

Author: @DS
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from collections import defaultdict
from typing import Any, Iterable, List, Mapping

import redis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PREFIX

logger = logging.getLogger(__name__)

# A stale key (aggregator crashed/stopped) should read as "no live data"
# downstream rather than silently serve an arbitrarily old candle forever.
CANDLE_KEY_TTL_SECONDS = 120
# The same reasoning, for a stopped indicator job.
INDICATOR_KEY_TTL_SECONDS = 120
# Bars per symbol and timeframe a batch publishes at most: its newest ones,
# which are all a live batch ever changes (the bar in progress, plus up to
# two not yet settled; see indicator_engine.py). A bigger batch is a warm-up
# or catch-up rewriting history, which charts get over REST, not pushed one
# bar at a time.
INDICATOR_PUBLISH_BARS = 3
# The newest footprint grid per symbol, like the candle key.
FOOTPRINT_KEY_TTL_SECONDS = 120
# Each live minute's levels, kept while the minute can still change (it
# forms for 60 s, then late trades may revise it within the watermark).
FOOTPRINT_LEVELS_TTL_SECONDS = 180


def _json_default(value: Any) -> str:
    """Same Decimal/datetime handling as ingestion/producer.py's _json_default."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class CandleRedisSink:
    """Writes the latest candle per symbol/timeframe to Redis as a JSON string."""

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        timeframe: str = "1m",
        ttl_seconds: int = CANDLE_KEY_TTL_SECONDS,
        prefix: str = REDIS_PREFIX,
    ) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._timeframe = timeframe
        self._ttl_seconds = ttl_seconds
        self._prefix = prefix

    def write_candle(self, candle: Mapping[str, Any]) -> None:
        """Cache this candle's current fields and publish the update for live subscribers."""
        symbol = candle["symbol"]
        value = json.dumps(dict(candle), default=_json_default)

        self._client.set(f"{self._prefix}candle:{symbol}:{self._timeframe}", value, ex=self._ttl_seconds)
        self._client.publish(f"{self._prefix}candle_updates:{symbol}:{self._timeframe}", value)


def plain_decimal(value: Decimal) -> str:
    """A decimal as plain digits without trailing zeros: 97000, 0.25 (never 9.7E+4)."""
    text = format(value.normalize(), "f")
    return "0" if text in ("-0", "") else text


class FootprintRedisSink:
    """
    Writes the live footprint for serving, a minute's whole grid at a time.

    Update mode hands a batch only the levels it changed, but a client
    needs a minute's every level. So each live minute's levels collect in a
    hash, `footprint_levels:{symbol}:1m:{window start, Unix seconds}`
    (field: the level's price, value: "buy sell"), and every minute a batch
    touched is sent out whole, read back from its hash in the same
    pipeline:

      - PUBLISH `footprint_updates:{symbol}:1m` for clients already
        connected (backend/routers/footprint.py);
      - SET `footprint:{symbol}:1m` (TTL'd) with the newest minute's grid,
        for clients that connect mid-minute. A batch that only revises an
        older minute (a late trade) leaves it alone.

    Each message: {"symbol", "window_start" (ISO 8601, UTC), "levels":
    [[price, buy_volume, sell_volume], ...]}, prices ascending, every
    number a plain decimal string ("97000", "1.25"). A whole grid rather
    than the changed levels, so a message never depends on the ones
    before it: a client replaces that minute's grid with it.
    """

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT, prefix: str = REDIS_PREFIX) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._prefix = prefix
        self._newest = {}  # symbol -> newest window_start cached under footprint:{symbol}:1m

    def write_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Rows of symbol, window_start (aware UTC), price, buy_volume and sell_volume (Decimals), any order."""
        windows = defaultdict(dict)
        for row in rows:
            levels = windows[(row["symbol"], row["window_start"])]
            levels[plain_decimal(row["price"])] = f"{plain_decimal(row['buy_volume'])} {plain_decimal(row['sell_volume'])}"
        if not windows:
            return

        order = sorted(windows, key=lambda key: key[1])  # oldest first: live clients never see time go back
        pipe = self._client.pipeline(transaction=False)
        for symbol, window_start in order:
            key = f"{self._prefix}footprint_levels:{symbol}:1m:{int(window_start.timestamp())}"
            pipe.hset(key, mapping=windows[(symbol, window_start)])
            pipe.expire(key, FOOTPRINT_LEVELS_TTL_SECONDS)
            pipe.hgetall(key)
        grids = pipe.execute()[2::3]

        pipe = self._client.pipeline(transaction=False)
        for (symbol, window_start), grid in zip(order, grids):
            levels = sorted(
                ([price, *value.split(" ")] for price, value in grid.items()), key=lambda level: Decimal(level[0])
            )
            message = json.dumps({"symbol": symbol, "window_start": window_start.isoformat(), "levels": levels})
            pipe.publish(f"{self._prefix}footprint_updates:{symbol}:1m", message)
            if self._newest.get(symbol) is None or window_start >= self._newest[symbol]:
                self._newest[symbol] = window_start
                pipe.set(f"{self._prefix}footprint:{symbol}:1m", message, ex=FOOTPRINT_KEY_TTL_SECONDS)
        pipe.execute()


class IndicatorRedisSink:
    """
    Writes indicator values for live serving, the way CandleRedisSink writes
    candles: per symbol and timeframe, a TTL'd SET of the newest bar
    (`indicators:{symbol}:{timeframe}`) for clients that connect mid-bar, and
    a PUBLISH of each changed bar (`indicator_updates:{symbol}:{timeframe}`)
    for clients already connected (backend/routers/indicators.py).

    Each message: {"symbol", "timeframe", "window_start" (ISO 8601, UTC),
    "indicators": {"ema_20": value or null, ...}} — the `indicators` object
    has the same shape GET /candles gives each candle.
    """

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        ttl_seconds: int = INDICATOR_KEY_TTL_SECONDS,
        prefix: str = REDIS_PREFIX,
    ) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._ttl_seconds = ttl_seconds
        self._prefix = prefix

    def write_rows(self, rows: Iterable[Mapping[str, Any]], columns: List[str]) -> None:
        """`rows` (symbol, timeframe, window_start and `columns`) oldest first within each symbol and timeframe."""
        by_series = defaultdict(list)
        for row in rows:
            by_series[(row["symbol"], row["timeframe"])].append(row)

        pipe = self._client.pipeline(transaction=False)
        for (symbol, timeframe), series in by_series.items():
            for row in series[-INDICATOR_PUBLISH_BARS:]:
                value = json.dumps(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "window_start": row["window_start"],
                        "indicators": {column: row[column] for column in columns},
                    },
                    default=_json_default,
                )
                pipe.publish(f"{self._prefix}indicator_updates:{symbol}:{timeframe}", value)
            pipe.set(f"{self._prefix}indicators:{symbol}:{timeframe}", value, ex=self._ttl_seconds)
        pipe.execute()


def announce_ready(job: str, host: str = REDIS_HOST, port: int = REDIS_PORT, prefix: str = REDIS_PREFIX) -> None:
    """
    SET `{prefix}ready:{job}` (TTL-less, the current time): this job's
    queries are running. replay/ waits for its jobs' keys before it starts a
    run's clock, so the first replayed minutes aren't played while Spark is
    still starting; clearing the replay pipeline's keys before the next run
    clears these too. Live jobs don't call it.
    """
    redis.Redis(host=host, port=port, decode_responses=True).set(
        f"{prefix}ready:{job}", datetime.now(timezone.utc).isoformat()
    )
