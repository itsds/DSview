"""
candles.py

REST endpoint for historical candles — the counterpart to routers/ws.py's
live push. Reads the Postgres serving copy (candles_1m) populated by
streaming/postgres_sink.py; deliberately never touches Redis, which only
ever holds the current candle and has no history to serve.

Bigger timeframes are rolled up from candles_1m at query time (see
postgres_client.py). `before` pages backwards for the chart's infinite
scroll-back: pass the window_start of the oldest candle already loaded.

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query

from postgres_client import CandleHistoryReader

router = APIRouter()

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
DEFAULT_LIMIT = 200
# One day of 1m candles — what frontend/src/hooks/useTimeframeCandle.js
# fetches to seed a live 1D candle that's already in progress.
MAX_LIMIT = 1440

# Must match frontend/src/lib/timeframes.js's TIMEFRAMES.
TIMEFRAMES = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1D": timedelta(days=1),
}


@router.get("/candles/{symbol}")
def get_candle_history(
    symbol: str,
    timeframe: str = "1m",
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    before: datetime | None = None,
) -> list[dict]:
    """Return up to `limit` candles for `symbol` at `timeframe`, oldest first.

    With `before`, only candles whose window starts before it.
    """
    bucket = TIMEFRAMES.get(timeframe)
    if bucket is None:
        raise HTTPException(status_code=422, detail=f"timeframe must be one of {list(TIMEFRAMES)}")
    if before is not None and before.tzinfo is None:
        before = before.replace(tzinfo=timezone.utc)  # a naive timestamp means UTC, like everything else here

    reader = CandleHistoryReader()
    try:
        return reader.get_candles(symbol, limit=limit, bucket=bucket, before=before)
    finally:
        reader.close()
