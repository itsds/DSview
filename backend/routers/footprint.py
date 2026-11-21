"""
footprint.py

The volume footprint's endpoints (Phase 3b): history and volume profiles
from Postgres's footprint_1m, live minutes from Redis — the same split as
candles, for the same reason. footprint_1m stores $1 levels per minute
(config/settings.py's FOOTPRINT_PRICE_STEP); every read merges them into
the rows and bars it asks for (postgres_client.py's FootprintReader).

- `GET /footprint/{symbol}?timeframe=5m&start=…&end=…&row_size=10`: the
  footprint of every bar from `start` to `end` (`end` defaults to now;
  both are widened to whole bars), on a footprint timeframe (1m to 1h,
  FOOTPRINT_TIMEFRAMES), in rows `row_size` wide (a multiple of the step,
  $1 by default). At most MAX_BARS bars per request: the chart only asks
  for the bars on screen, once zoomed in far enough to read them.

      [{"window_start": "2026-11-20T10:15:00Z",
        "levels": [["97000", "1.25", "0.5"], ["97010", "0", "2"]]}, ...]

  Each level is [price, buy_volume, sell_volume]: the row's lower bound,
  then the taker volume that lifted the ask and hit the bid, as plain
  decimal strings. Oldest bar first, prices ascending, bars without trades
  left out.

- `GET /footprint/{symbol}/profile?start=…&end=…&rows=24` (or
  `&row_size=…`): the volume profile of any time range on any timeframe,
  for the chart's volume profile tools. `rows` asks for about that many
  rows over the range's traded prices (TradingView's "number of rows"
  layout); `row_size` fixes their width instead. Also says which minutes
  of the range have any footprint (`first_minute`, `last_minute`): the
  backfill covers 30 days by default, so an older range is partial.

- `/ws/footprint/{symbol}`: the minute in progress, live. On connect the
  newest minute's cached grid, then every grid FootprintRedisSink
  publishes, each the whole minute so far:

      {"symbol": "BTCUSDT", "window_start": "2026-11-20T10:16:00+00:00",
       "levels": [["97000", "1.25", "0.5"], ...]}

  1m only, like /ws/candles: the client rolls minutes up into its bar and
  merges levels into its row size itself. Mostly the minute in progress;
  a late trade can revise the one before, so place each by window_start.
  Pushes through push.py; 1011 if Redis is unreachable.

Author: @DS
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, HTTPException, Query, WebSocket, status

from config.settings import FOOTPRINT_PRICE_STEP, FOOTPRINT_TIMEFRAMES, TIMEFRAMES

from postgres_client import FootprintReader
from redis_client import CandleRedisReader
from routers.push import push_until_disconnect

logger = logging.getLogger(__name__)

router = APIRouter()

# Bars per footprint request: several screens' worth at the widths a
# footprint is readable at.
MAX_BARS = 500
DEFAULT_PROFILE_ROWS = 24  # TradingView's default
MAX_PROFILE_ROWS = 1000

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment  # naive means UTC


def _bar_floor(moment: datetime, size: timedelta) -> datetime:
    return _EPOCH + (moment - _EPOCH) // size * size


def _bar_ceil(moment: datetime, size: timedelta) -> datetime:
    floor = _bar_floor(moment, size)
    return floor if floor == moment else floor + size


def _row_size(text: str | None) -> Decimal | None:
    """A row size from the query string: a positive multiple of FOOTPRINT_PRICE_STEP, else 422."""
    if text is None:
        return None
    try:
        size = Decimal(text)
    except InvalidOperation:
        size = None
    if size is None or not size.is_finite() or size <= 0 or size % FOOTPRINT_PRICE_STEP != 0:
        raise HTTPException(
            status_code=422, detail=f"row_size must be a positive multiple of the footprint's step, {FOOTPRINT_PRICE_STEP}"
        )
    return size


def _range(start: datetime, end: datetime | None) -> tuple[datetime, datetime]:
    start = _utc(start)
    end = _utc(end) if end is not None else datetime.now(timezone.utc)
    if end <= start:
        raise HTTPException(status_code=422, detail="end must be after start")
    return start, end


@router.get("/footprint/{symbol}")
def get_footprint(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    timeframe: str = "1m",
    row_size: str = str(FOOTPRINT_PRICE_STEP),
) -> list[dict]:
    """The footprint of `symbol`'s `timeframe` bars from `start` to `end`, in rows of `row_size`."""
    if timeframe not in FOOTPRINT_TIMEFRAMES:
        raise HTTPException(status_code=422, detail=f"the footprint's timeframes are {list(FOOTPRINT_TIMEFRAMES)}")
    size = _row_size(row_size)
    start, end = _range(start, end)
    bar = TIMEFRAMES[timeframe]
    start, end = _bar_floor(start, bar), _bar_ceil(end, bar)
    if (end - start) / bar > MAX_BARS:
        raise HTTPException(status_code=422, detail=f"at most {MAX_BARS} bars per request")

    reader = FootprintReader()
    try:
        return reader.get_bars(symbol, timeframe, start, end, size)
    finally:
        reader.close()


@router.get("/footprint/{symbol}/profile")
def get_profile(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    rows: int = Query(DEFAULT_PROFILE_ROWS, ge=1, le=MAX_PROFILE_ROWS),
    row_size: str | None = None,
) -> dict:
    """The volume profile of `symbol` over [start, end): about `rows` rows, or rows `row_size` wide."""
    size = _row_size(row_size)
    start, end = _range(start, end)
    reader = FootprintReader()
    try:
        profile = reader.get_profile(symbol, start, end, size, rows)
    finally:
        reader.close()
    return {"symbol": symbol, "start": start, "end": end, **profile}


@router.websocket("/ws/footprint/{symbol}")
async def footprint_updates(websocket: WebSocket, symbol: str) -> None:
    """Push `symbol`'s live footprint, a minute's whole grid at a time, until the client disconnects."""
    await websocket.accept()
    reader = CandleRedisReader()
    try:
        error = await push_until_disconnect(websocket, _push_footprint(websocket, reader, symbol))
    finally:
        await reader.close()

    if error is None:
        logger.info("Client disconnected from footprint stream for %s", symbol)
        return

    logger.error("Footprint stream for %s failed: %r", symbol, error)
    await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="footprint updates unavailable")


async def _push_footprint(websocket: WebSocket, reader: CandleRedisReader, symbol: str) -> None:
    """Send the cached newest minute, if any, then every grid as it's published."""
    latest = await reader.get_latest_footprint(symbol)
    if latest is not None:
        await websocket.send_text(latest)

    async for update in reader.subscribe_footprint_updates(symbol):
        await websocket.send_text(update)
