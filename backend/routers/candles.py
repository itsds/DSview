"""
candles.py

REST endpoint for historical candles — the counterpart to routers/ws.py's
live push. Reads the Postgres serving copy (candles_1m) populated by
streaming/postgres_sink.py; deliberately never touches Redis, which only
ever holds the current candle and has no history to serve.

Author: @DS
"""

from __future__ import annotations

from fastapi import APIRouter

from postgres_client import CandleHistoryReader

router = APIRouter()

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded;
# also make timeframe a query param once more than "1m" exists.
DEFAULT_LIMIT = 200


@router.get("/candles/{symbol}")
def get_candle_history(symbol: str, limit: int = DEFAULT_LIMIT) -> list[dict]:
    """Return up to `limit` recent 1-minute candles for `symbol`, oldest first."""
    reader = CandleHistoryReader()
    try:
        return reader.get_recent_candles(symbol, limit=limit)
    finally:
        reader.close()
