"""
Tests for the footprint endpoints (backend/routers/footprint.py +
postgres_client.py's FootprintReader + redis_client.py) over real Postgres
and Redis: GET /footprint/{symbol}, GET /footprint/{symbol}/profile and
/ws/footprint/{symbol}.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import partial

import pytest
from starlette.websockets import WebSocketDisconnect

from redis_client import CandleRedisReader

from helpers import parse_utc, wait_for

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def ten_minutes(pg, symbol):
    """
    footprint_1m for 10:00–10:09, no trades at 10:05. Minute i has three $1
    levels, 100 + i, 101 + i and 109 + i, with buy = i + 1 and sell = 0.5
    on each: so 10:00's levels are 100, 101 and 109.
    """
    for i in range(10):
        if i == 5:
            continue
        for price in (100 + i, 101 + i, 109 + i):
            pg.execute(
                "INSERT INTO footprint_1m VALUES (%s, %s, %s, %s, %s)",
                (symbol, BASE + timedelta(minutes=i), price, i + 1, Decimal("0.5")),
            )
    yield
    pg.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))


def _iso(minutes: float) -> str:
    return (BASE + timedelta(minutes=minutes)).isoformat()


def _bars(response):
    assert response.status_code == 200, response.text
    return [(parse_utc(bar["window_start"]), bar["levels"]) for bar in response.json()]


def test_minutes_at_dollar_rows_as_plain_decimal_strings(client, ten_minutes, symbol):
    bars = _bars(client.get(f"/footprint/{symbol}", params={"start": _iso(0), "end": _iso(2)}))

    assert bars == [
        (BASE, [["100", "1", "0.5"], ["101", "1", "0.5"], ["109", "1", "0.5"]]),
        (BASE + timedelta(minutes=1), [["101", "2", "0.5"], ["102", "2", "0.5"], ["110", "2", "0.5"]]),
    ]


def _expected_bar(minutes, row):
    """The fixture's levels for these minutes, merged into `row`-wide rows, as the API writes them."""
    cells = {}
    for i in minutes:
        for price in (100 + i, 101 + i, 109 + i):
            level = price // row * row
            buy, sell = cells.get(level, (0, Decimal(0)))
            cells[level] = (buy + i + 1, sell + Decimal("0.5"))
    return [[str(level), str(buy), str(sell.normalize())] for level, (buy, sell) in sorted(cells.items())]


def test_bigger_bars_and_coarser_rows_are_merged_and_partial_bars_widened(client, ten_minutes, symbol):
    # 10:02 to 10:07 widens to the whole 10:00 and 10:05 bars.
    bars = _bars(
        client.get(f"/footprint/{symbol}", params={"timeframe": "5m", "row_size": "5", "start": _iso(2), "end": _iso(7)})
    )

    assert bars == [
        (BASE, _expected_bar(range(5), 5)),
        (BASE + timedelta(minutes=5), _expected_bar([6, 7, 8, 9], 5)),  # no trades at 10:05
    ]
    # Once by hand: the $100 row is levels 100..104, two in each of minutes
    # 0..3 (buy 1, 2, 3, 4 each) and one in minute 4 (buy 5), 0.5 sold on each.
    assert bars[0][1][0] == ["100", "25", "4.5"]


def test_bars_without_trades_are_left_out(client, ten_minutes, symbol):
    bars = _bars(client.get(f"/footprint/{symbol}", params={"start": _iso(4), "end": _iso(7)}))
    assert [start for start, _ in bars] == [BASE + timedelta(minutes=m) for m in (4, 6)]


def test_naive_times_mean_utc(client, ten_minutes, symbol):
    bars = _bars(client.get(f"/footprint/{symbol}", params={"start": "2026-11-20T10:08:00", "end": "2026-11-20T10:10:00"}))
    assert [start for start, _ in bars] == [BASE + timedelta(minutes=8), BASE + timedelta(minutes=9)]


def test_end_defaults_to_now(client, pg, symbol):
    minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    pg.execute("INSERT INTO footprint_1m VALUES (%s, %s, 97000, 1, 2)", (symbol, minute))
    try:
        bars = _bars(client.get(f"/footprint/{symbol}", params={"start": (minute - timedelta(minutes=3)).isoformat()}))
    finally:
        pg.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))
    assert bars == [(minute, [["97000", "1", "2"]])]


@pytest.mark.parametrize(
    "params",
    [
        {"timeframe": "4h"},  # plain candles only (kickoff decision 4)
        {"timeframe": "2m"},
        {"row_size": "0"},
        {"row_size": "-5"},
        {"row_size": "1.5"},  # not a multiple of the $1 step
        {"row_size": "abc"},
        {"row_size": "NaN"},
        {"end": _iso(-1)},  # before start
        {"end": _iso(501)},  # more than MAX_BARS 1m bars
    ],
)
def test_bad_requests_are_422(client, symbol, params):
    response = client.get(f"/footprint/{symbol}", params={"start": _iso(0), "end": _iso(10), **params})
    assert response.status_code == 422, response.text


def test_a_profile_sums_the_range_into_about_the_rows_asked_for(client, ten_minutes, symbol):
    response = client.get(f"/footprint/{symbol}/profile", params={"start": _iso(0), "end": _iso(10), "rows": 4})
    assert response.status_code == 200, response.text
    profile = response.json()

    # Prices 100..118 traded: 19 $1 levels in about 4 rows is $5 rows.
    assert profile["row_size"] == "5"
    assert parse_utc(profile["first_minute"]) == BASE and parse_utc(profile["last_minute"]) == BASE + timedelta(minutes=9)
    assert [level[0] for level in profile["levels"]] == ["100", "105", "110", "115"]
    total = sum(Decimal(buy) + Decimal(sell) for _, buy, sell in profile["levels"])
    minutes = [i for i in range(10) if i != 5]
    assert total == sum(3 * (i + 1) + 3 * Decimal("0.5") for i in minutes)


def test_a_profile_can_fix_its_row_size_and_says_how_much_of_the_range_it_covers(client, ten_minutes, symbol):
    profile = client.get(
        f"/footprint/{symbol}/profile",
        params={"start": "2026-11-20T09:00:00Z", "end": _iso(2), "row_size": "100"},
    ).json()
    assert profile["row_size"] == "100"
    assert parse_utc(profile["first_minute"]) == BASE  # the hour before 10:00 has no footprint
    assert parse_utc(profile["last_minute"]) == BASE + timedelta(minutes=1)
    assert profile["levels"] == [["100", "9", "3"]]  # minutes 0 and 1: three levels each, buy 1 and 2


def test_a_profile_of_a_range_without_footprint_is_empty(client, ten_minutes, symbol):
    profile = client.get(f"/footprint/{symbol}/profile", params={"start": _iso(60), "end": _iso(90)}).json()
    assert profile["levels"] == [] and profile["first_minute"] is None and profile["last_minute"] is None


# --- /ws/footprint ------------------------------------------------------------


def _subscribers(redis_client, channel: str) -> int:
    return dict(redis_client.pubsub_numsub(channel))[channel]


@pytest.fixture
def reader_closed(monkeypatch) -> threading.Event:
    closed = threading.Event()

    class TrackedReader(CandleRedisReader):
        async def close(self) -> None:
            await super().close()
            closed.set()

    monkeypatch.setattr("routers.footprint.CandleRedisReader", TrackedReader)
    return closed


def _grid(symbol, start, levels):
    return json.dumps({"symbol": symbol, "window_start": start, "levels": levels})


def test_ws_sends_the_cached_minute_then_every_published_grid(client, redis_client, symbol, reader_closed):
    channel = f"footprint_updates:{symbol}:1m"
    cached = _grid(symbol, "2026-11-20T10:16:00+00:00", [["97000", "1", "0"]])
    redis_client.set(f"footprint:{symbol}:1m", cached, ex=60)
    try:
        with client.websocket_connect(f"/ws/footprint/{symbol}") as ws:
            assert ws.receive_text() == cached

            wait_for(lambda: _subscribers(redis_client, channel) == 1, timeout=5)
            update = _grid(symbol, "2026-11-20T10:16:00+00:00", [["97000", "1", "0"], ["97001", "0", "2"]])
            redis_client.publish(channel, update)
            assert ws.receive_text() == update

            ws.close()
            wait_for(lambda: _subscribers(redis_client, channel) == 0, timeout=5, message="subscription outlived the socket")
            assert reader_closed.wait(timeout=5), "the handler never closed its Redis client"
    finally:
        redis_client.delete(f"footprint:{symbol}:1m")


def test_ws_closes_with_1011_when_redis_is_unreachable(client, monkeypatch, closed_port, symbol):
    monkeypatch.setattr("routers.footprint.CandleRedisReader", partial(CandleRedisReader, port=closed_port))

    with client.websocket_connect(f"/ws/footprint/{symbol}") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1011
