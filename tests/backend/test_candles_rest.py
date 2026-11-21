"""Tests for GET /candles/{symbol} (backend/routers/candles.py + postgres_client.py) over real Postgres, indicators included."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from helpers import parse_utc

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def ten_minutes(pg, symbol):
    """candles_1m rows for 10:00–10:09: open = 100 + i, high = open + 5, low = open − 5, close = open + 1."""
    for i in range(10):
        start = BASE + timedelta(minutes=i)
        pg.execute(
            """
            INSERT INTO candles_1m (symbol, window_start, window_end, open, high, low, close, volume, trade_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (symbol, start, start + timedelta(minutes=1), 100 + i, 105 + i, 95 + i, 101 + i, 1 + i, 10),
        )
    yield
    pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
    pg.execute("DELETE FROM candle_indicators WHERE symbol = %s", (symbol,))


def _starts(candles):
    return [parse_utc(c["window_start"]) for c in candles]


def _ohlc(candle):
    # NUMERIC columns arrive as decimal strings ("109.000000000000000000");
    # the frontend reads them with Number().
    return tuple(Decimal(str(candle[field])) for field in ("open", "high", "low", "close"))


def test_newest_candles_come_back_oldest_first(client, ten_minutes, symbol):
    response = client.get(f"/candles/{symbol}", params={"limit": 3})

    assert response.status_code == 200
    candles = response.json()
    assert _starts(candles) == [BASE + timedelta(minutes=m) for m in (7, 8, 9)]
    assert _ohlc(candles[-1]) == (109, 114, 104, 110)


def test_bigger_timeframes_roll_up_epoch_aligned_buckets(client, ten_minutes, symbol):
    candles = client.get(f"/candles/{symbol}", params={"timeframe": "5m"}).json()

    assert _starts(candles) == [BASE, BASE + timedelta(minutes=5)]
    first = candles[0]  # 10:00–10:04
    assert parse_utc(first["window_end"]) == BASE + timedelta(minutes=5)
    assert _ohlc(first) == (100, 109, 95, 105)
    assert Decimal(str(first["volume"])) == 1 + 2 + 3 + 4 + 5
    assert first["trade_count"] == 50


def test_before_pages_backwards_and_a_naive_timestamp_means_utc(client, ten_minutes, symbol):
    naive = (BASE + timedelta(minutes=5)).replace(tzinfo=None).isoformat()  # "2026-11-20T10:05:00"
    candles = client.get(f"/candles/{symbol}", params={"timeframe": "1m", "before": naive}).json()
    assert _starts(candles) == [BASE + timedelta(minutes=m) for m in range(5)]

    older = client.get(f"/candles/{symbol}", params={"timeframe": "5m", "before": (BASE + timedelta(minutes=5)).isoformat()})
    assert _starts(older.json()) == [BASE]


def _store_indicators(pg, symbol, timeframe, start, ema_20, ema_50=None, ema_200=None):
    pg.execute(
        """
        INSERT INTO candle_indicators (symbol, timeframe, window_start, ema_20, ema_50, ema_200)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (symbol, timeframe, start, ema_20, ema_50, ema_200),
    )


def test_each_candle_carries_its_own_timeframes_indicators(client, ten_minutes, pg, symbol):
    _store_indicators(pg, symbol, "1m", BASE + timedelta(minutes=9), 109.123456789012, 105.5)
    _store_indicators(pg, symbol, "5m", BASE + timedelta(minutes=5), 104.25)
    _store_indicators(pg, symbol, "5m", BASE + timedelta(minutes=9), 999.0)  # not a 5m bar start: never matched

    minute = client.get(f"/candles/{symbol}", params={"limit": 2}).json()
    assert [c["indicators"] for c in minute] == [
        {"ema_20": None, "ema_50": None, "ema_200": None},  # 10:08: nothing computed
        {"ema_20": 109.123456789012, "ema_50": 105.5, "ema_200": None},  # the double, exactly
    ]

    five = client.get(f"/candles/{symbol}", params={"timeframe": "5m"}).json()
    assert [c["indicators"]["ema_20"] for c in five] == [None, 104.25]
    assert _ohlc(five[1]) == (105, 114, 100, 110)  # the join leaves the candles as they were


def test_unknown_timeframe_and_out_of_range_limit_are_rejected(client, symbol):
    assert client.get(f"/candles/{symbol}", params={"timeframe": "2m"}).status_code == 422
    assert client.get(f"/candles/{symbol}", params={"limit": 0}).status_code == 422
    assert client.get(f"/candles/{symbol}", params={"limit": 1441}).status_code == 422


def test_unknown_symbol_is_an_empty_list(client, symbol, postgres_schema):
    assert client.get(f"/candles/{symbol}").json() == []


def test_rest_allows_the_vite_dev_server_origin(client, symbol, postgres_schema):
    response = client.get(f"/candles/{symbol}", headers={"Origin": "http://localhost:5173"})
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}
