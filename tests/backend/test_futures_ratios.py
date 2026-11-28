"""
Tests for the long/short ratios' endpoints (backend/routers/futures_ratios.py
and postgres_client.py's FuturesRatioReader) over real Postgres and Redis:
every timeframe's roll-up checked against one worked out here from the same
periods, what's refused, and the live socket.
"""

from __future__ import annotations

import json
import threading
import zlib
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from functools import partial

import pytest
from starlette.websockets import WebSocketDisconnect

from config.settings import FUTURES_RATIO_KINDS, TIMEFRAMES

from redis_client import CandleRedisReader
from redis_sink import FuturesRatioRedisSink

from helpers import parse_utc, wait_for

START = datetime(2026, 11, 19, 22, 0, tzinfo=timezone.utc)  # 2 hours before a UTC midnight
PERIOD = timedelta(minutes=5)
PERIODS = 60  # five hours: across the midnight, several 1h and 4h bars, two days


def plain(value: Decimal) -> str:
    return format(value.normalize(), "f")


def period_values(symbol: str, kind: str, i: int):
    """(ratio, long, short) for a period, as Binance would send them."""
    h = zlib.crc32(f"{symbol}{kind}{i}".encode())
    if kind == "taker_volume":
        buy = Decimal(100 + h % 900) + Decimal(h % 97) / 100
        sell = Decimal(100 + h // 11 % 900) + Decimal(h % 89) / 100
        return (buy / sell).quantize(Decimal("0.0001")), buy, sell
    long = Decimal(3000 + h % 4000) / 10_000
    short = 1 - long
    return (long / short).quantize(Decimal("0.0001")), long, short


@pytest.fixture
def periods(pg, postgres_schema, symbol):
    """PERIODS periods of all four ratios for `symbol`, with a hole: no top_accounts in the third hour."""
    rows = []
    for i in range(PERIODS):
        for kind in FUTURES_RATIO_KINDS:
            if kind == "top_accounts" and 24 <= i < 36:
                continue
            ratio, long, short = period_values(symbol, kind, i)
            rows.append((symbol, START + i * PERIOD, kind, ratio, long, short))
    with pg.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO futures_ratios (symbol, period_start, kind, ratio, long_value, short_value) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
    return rows


def expected_bars(rows, bar: timedelta):
    """The roll-up, worked out by hand: last period for the snapshots, buy over sell for taker volume."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    grouped = {}
    for _, start, kind, ratio, long, short in rows:
        key = epoch + (start - epoch) // bar * bar
        grouped.setdefault(key, {}).setdefault(kind, []).append((start, ratio, long, short))
    bars = []
    for key in sorted(grouped):
        ratios = {}
        for kind, periods in grouped[key].items():
            periods.sort()
            if kind == "taker_volume":
                buy, sell = sum(p[2] for p in periods), sum(p[3] for p in periods)
                ratio = periods[0][1] if len(periods) == 1 else (buy / sell).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                ratios[kind] = {"ratio": plain(ratio), "long": plain(buy), "short": plain(sell)}
            else:
                _, ratio, long, short = periods[-1]
                ratios[kind] = {"ratio": plain(ratio), "long": plain(long), "short": plain(short)}
        bars.append({"window_start": key, "ratios": ratios})
    return bars


@pytest.mark.parametrize("timeframe", ["5m", "15m", "1h", "4h", "1D"])
def test_every_timeframe_rolls_the_periods_up(client, periods, symbol, timeframe):
    response = client.get(
        f"/futures-ratios/{symbol}",
        params={"timeframe": timeframe, "start": "2026-11-19T00:00:00Z", "end": "2026-11-21T00:00:00Z"},
    )
    assert response.status_code == 200, response.text
    got = [{**bar, "window_start": parse_utc(bar["window_start"])} for bar in response.json()]
    assert got == expected_bars(periods, TIMEFRAMES[timeframe])


def test_on_5m_every_bar_is_binances_own_period(client, periods, symbol):
    params = {"timeframe": "5m", "start": START.isoformat(), "end": (START + PERIODS * PERIOD).isoformat()}
    bars = client.get(f"/futures-ratios/{symbol}", params=params).json()
    assert len(bars) == PERIODS
    _, start, kind, ratio, long, short = periods[0]
    assert bars[0]["ratios"][kind] == {"ratio": plain(ratio), "long": plain(long), "short": plain(short)}
    # The hole: those bars have the other three ratios only.
    assert set(bars[24]["ratios"]) == set(FUTURES_RATIO_KINDS) - {"top_accounts"}


def test_a_range_is_widened_to_whole_bars(client, periods, symbol):
    bars = client.get(
        f"/futures-ratios/{symbol}",
        params={"timeframe": "1h", "start": "2026-11-19T22:30:00Z", "end": "2026-11-19T23:10:00Z"},
    ).json()
    assert [bar["window_start"] for bar in bars] == ["2026-11-19T22:00:00Z", "2026-11-19T23:00:00Z"]


@pytest.mark.parametrize(
    ("params", "detail"),
    [
        ({"timeframe": "1m"}, "ask for 5m on 1m charts"),
        ({"timeframe": "2h"}, "timeframe must be one of"),
        ({"timeframe": "5m", "start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"}, "at most 1000 bars"),
        ({"timeframe": "5m", "start": "2026-02-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"}, "end must be after start"),
    ],
)
def test_what_is_refused(client, symbol, params, detail):
    response = client.get(f"/futures-ratios/{symbol}", params={"start": "2026-11-20T00:00:00Z", **params})
    assert response.status_code == 422 and detail in response.text


def test_a_symbol_without_ratios_has_no_bars(client, postgres_schema, symbol):
    params = {"start": START.isoformat(), "end": (START + timedelta(days=1)).isoformat()}
    assert client.get(f"/futures-ratios/{symbol}", params=params).json() == []


# --- the live socket ---------------------------------------------------------


def _subscribers(redis_client, channel: str) -> int:
    return dict(redis_client.pubsub_numsub(channel))[channel]


@pytest.fixture
def reader_closed(monkeypatch) -> threading.Event:
    closed = threading.Event()

    class TrackedReader(CandleRedisReader):
        async def close(self) -> None:
            await super().close()
            closed.set()

    monkeypatch.setattr("routers.futures_ratios.CandleRedisReader", TrackedReader)
    return closed


def test_sends_the_newest_bar_then_every_bar_a_change_touches(client, pg, periods, redis_client, symbol, reader_closed):
    channel = f"futures_ratio_updates:{symbol}"
    with client.websocket_connect(f"/ws/futures-ratios/{symbol}?timeframe=1h") as ws:
        newest = json.loads(ws.receive_text())
        hourly = expected_bars(periods, timedelta(hours=1))
        assert newest == {
            "symbol": symbol,
            "timeframe": "1h",
            "window_start": hourly[-1]["window_start"].isoformat(),
            "ratios": hourly[-1]["ratios"],
        }

        # The next period, announced as the consumer announces it.
        wait_for(lambda: _subscribers(redis_client, channel) == 1, timeout=5)
        nxt = START + PERIODS * PERIOD
        pg.execute(
            "INSERT INTO futures_ratios VALUES (%s, %s, 'global_accounts', 2.5, 0.7143, 0.2857)", (symbol, nxt)
        )
        FuturesRatioRedisSink().publish_changes([(symbol, nxt)])
        pushed = json.loads(ws.receive_text())
        assert pushed["window_start"] == "2026-11-20T03:00:00+00:00"
        assert pushed["ratios"] == {"global_accounts": {"ratio": "2.5", "long": "0.7143", "short": "0.2857"}}

        # A catch-up touching the whole five hours pushes the newest bars only, oldest first.
        FuturesRatioRedisSink().publish_changes([(symbol, START), (symbol, nxt)])
        starts = [json.loads(ws.receive_text())["window_start"] for _ in range(6)]
        assert starts == [f"2026-11-{d}T{h:02d}:00:00+00:00" for d, h in [(19, 22), (19, 23), (20, 0), (20, 1), (20, 2), (20, 3)]]

        ws.close()
        wait_for(lambda: _subscribers(redis_client, channel) == 0, timeout=5, message="subscription outlived the socket")
        assert reader_closed.wait(timeout=5), "the handler never closed its Redis client"


def test_a_symbol_without_ratios_sends_nothing_until_one_comes(client, pg, postgres_schema, redis_client, symbol, reader_closed):
    with client.websocket_connect(f"/ws/futures-ratios/{symbol}?timeframe=5m") as ws:
        wait_for(lambda: _subscribers(redis_client, f"futures_ratio_updates:{symbol}") == 1, timeout=5)
        pg.execute("INSERT INTO futures_ratios VALUES (%s, %s, 'taker_volume', 1.5, 3, 2)", (symbol, START))
        FuturesRatioRedisSink().publish_changes([(symbol, START)])
        assert json.loads(ws.receive_text())["ratios"] == {"taker_volume": {"ratio": "1.5", "long": "3", "short": "2"}}
        ws.close()
        assert reader_closed.wait(timeout=5)


@pytest.mark.parametrize("timeframe", ["1m", "3m"])
def test_a_timeframe_it_doesnt_serve_is_closed_with_1008(client, symbol, timeframe):
    with client.websocket_connect(f"/ws/futures-ratios/{symbol}?timeframe={timeframe}") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1008


def test_closes_with_1011_when_redis_is_unreachable(client, monkeypatch, closed_port, symbol):
    monkeypatch.setattr("routers.futures_ratios.CandleRedisReader", partial(CandleRedisReader, port=closed_port))
    with client.websocket_connect(f"/ws/futures-ratios/{symbol}?timeframe=5m") as ws:
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_text()
    assert closed.value.code == 1011
