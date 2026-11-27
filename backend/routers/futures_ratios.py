"""
futures_ratios.py

The long/short ratios' endpoints (Phase 4c): Binance Futures' four ratios
for the symbol's USDⓈ-M perpetual, history from Postgres's futures_ratios
(5-minute periods, rolled up to the asked timeframe by postgres_client.py's
FuturesRatioReader) and changes pushed live, as streaming/jobs/
futures_ratios.py announces them on Redis. Live only: the replay has no
Futures feed, so there's no `?pipeline=`; a replayed chart asks for live's
ratios of its bars.

- `GET /futures-ratios/{symbol}?timeframe=1h&start=…&end=…`: every bar from
  `start` to `end` (`end` defaults to now; both widened to whole bars), at
  most MAX_BARS, oldest first, bars without any ratio left out:

      [{"window_start": "2026-11-20T10:00:00Z",
        "ratios": {"global_accounts": {"ratio": "1.9234", "long": "0.6579", "short": "0.3421"},
                   "top_accounts": {...}, "top_positions": {...},
                   "taker_volume": {"ratio": "1.12", "long": "1520.5", "short": "1357.1"}}}, ...]

  `ratio` is longs over shorts (buy over sell volume for taker_volume),
  `long` and `short` the shares long and short (0 to 1; taker volume's: the
  buy and sell volume, in the base asset), as plain decimal strings. The
  account and position ratios are each bar's last period; taker volume
  adds up over the bar. Timeframes: 5m to 1D (RATIO_TIMEFRAMES). A 1m
  chart asks for 5m and shows each period on the minutes it spans: there
  is nothing finer to show.

- `/ws/futures-ratios/{symbol}?timeframe=1h`: the newest bar on connect,
  then every bar whose ratios change, rolled up again from Postgres as each
  change is announced (a period every 5 minutes, live), each

      {"symbol": "BTCUSDT", "timeframe": "1h", "window_start": "...", "ratios": {...}}

  Usually the bar in progress; after the poller catches up, up to
  MAX_PUSH_BARS bars, oldest first, so place each by window_start. Pushes
  through push.py; 1008 for a timeframe it doesn't serve, 1011 if Redis or
  Postgres is unreachable.

Author: @DS
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, WebSocket, status

from config.settings import FUTURES_RATIO_PERIOD, LIVE_PIPELINE, TIMEFRAMES

from postgres_client import FuturesRatioReader
from redis_client import CandleRedisReader
from routers.push import push_until_disconnect

logger = logging.getLogger(__name__)

router = APIRouter()

# The chart timeframes the ratios are served at: those no finer than the
# period they're stored at.
RATIO_TIMEFRAMES = [name for name, length in TIMEFRAMES.items() if length >= FUTURES_RATIO_PERIOD]
# Bars per request: a few pages of the chart's 300 bars.
MAX_BARS = 1000
# Bars one announcement pushes at most: the newest ones. A bigger change (a
# catch-up after a restart) is history, which charts load over REST.
MAX_PUSH_BARS = 12

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment  # naive means UTC


def _bar_floor(moment: datetime, size: timedelta) -> datetime:
    return _EPOCH + (moment - _EPOCH) // size * size


def _timeframe_error(timeframe: str) -> str | None:
    if timeframe in RATIO_TIMEFRAMES:
        return None
    if timeframe in TIMEFRAMES:
        return f"the ratios are stored per {RATIO_TIMEFRAMES[0]} period: ask for {RATIO_TIMEFRAMES[0]} on {timeframe} charts"
    return f"timeframe must be one of {RATIO_TIMEFRAMES}"


def _reader() -> FuturesRatioReader:
    return FuturesRatioReader(dsn=LIVE_PIPELINE.postgres_dsn)


@router.get("/futures-ratios/{symbol}")
def get_futures_ratios(symbol: str, start: datetime, end: datetime | None = None, timeframe: str = "5m") -> list[dict]:
    """`symbol`'s ratios per `timeframe` bar from `start` to `end`."""
    error = _timeframe_error(timeframe)
    if error:
        raise HTTPException(status_code=422, detail=error)
    bar = TIMEFRAMES[timeframe]
    start = _bar_floor(_utc(start), bar)
    end = _utc(end) if end is not None else datetime.now(timezone.utc)
    end = _bar_floor(end, bar) + (bar if _bar_floor(end, bar) != end else timedelta(0))
    if end <= start:
        raise HTTPException(status_code=422, detail="end must be after start")
    if (end - start) / bar > MAX_BARS:
        raise HTTPException(status_code=422, detail=f"at most {MAX_BARS} bars per request")

    reader = _reader()
    try:
        return reader.get_bars(symbol, bar, start, end)
    finally:
        reader.close()


@router.websocket("/ws/futures-ratios/{symbol}")
async def futures_ratio_updates(websocket: WebSocket, symbol: str, timeframe: str = "5m") -> None:
    """Push `symbol`'s ratios at `timeframe` as they change, until the client disconnects."""
    await websocket.accept()
    error = _timeframe_error(timeframe)
    if error:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=error[:120])
        return

    redis = CandleRedisReader(prefix=LIVE_PIPELINE.redis_prefix)
    try:
        error = await push_until_disconnect(websocket, _push_ratios(websocket, redis, symbol, timeframe))
    finally:
        await redis.close()

    if error is None:
        logger.info("Client disconnected from the ratio stream for %s %s", symbol, timeframe)
        return
    logger.error("Ratio stream for %s %s failed: %r", symbol, timeframe, error)
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="ratio updates unavailable")


async def _push_ratios(websocket: WebSocket, redis: CandleRedisReader, symbol: str, timeframe: str) -> None:
    """Send the newest bar, if any, then every bar each announced change touches."""
    bar = TIMEFRAMES[timeframe]
    updates = redis.subscribe_futures_ratio_updates(symbol)
    try:
        await updates.__anext__()  # subscribed: a change from here on is pushed, so read the newest bar now
        newest = await asyncio.to_thread(_newest_bar, symbol, bar)
        if newest is not None:
            await websocket.send_text(_message(symbol, timeframe, newest))
        async for update in updates:
            change = json.loads(update)
            first = _bar_floor(datetime.fromisoformat(change["first_period"]), bar)
            last = _bar_floor(datetime.fromisoformat(change["last_period"]), bar)
            first = max(first, last - (MAX_PUSH_BARS - 1) * bar)
            for changed in await asyncio.to_thread(_bars, symbol, bar, first, last + bar):
                await websocket.send_text(_message(symbol, timeframe, changed))
    finally:
        await updates.aclose()


def _newest_bar(symbol: str, bar: timedelta) -> dict | None:
    reader = _reader()
    try:
        newest = reader.newest_period(symbol)
        if newest is None:
            return None
        start = _bar_floor(newest, bar)
        bars = reader.get_bars(symbol, bar, start, start + bar)
        return bars[-1] if bars else None
    finally:
        reader.close()


def _bars(symbol: str, bar: timedelta, start: datetime, end: datetime) -> list[dict]:
    reader = _reader()
    try:
        return reader.get_bars(symbol, bar, start, end)
    finally:
        reader.close()


def _message(symbol: str, timeframe: str, bar: dict) -> str:
    return json.dumps(
        {"symbol": symbol, "timeframe": timeframe, "window_start": bar["window_start"].isoformat(), "ratios": bar["ratios"]}
    )
