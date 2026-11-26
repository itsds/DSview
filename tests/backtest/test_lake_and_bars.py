"""
Tests for backtest/lake_history.py and bar_series.py: Bronze's klines as
the backfill writes them (tests/bronze_fakes.py, over fake_binance.py's
synthetic market) read back, rolled up into the chart's bars, with the
browser's own EMAs, and the footprint where aggTrades cover a bar.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from bar_series import build_bars, with_footprints
from candle_store import CandleStore
from fake_binance import MINUTE_MS, SyntheticMarket
from lake_history import NoHistory, read_footprint, read_minutes
from postgres_client import CandleHistoryReader

from bronze_fakes import DAY_MS, write_agg_trades, write_klines
from helpers import math_js_ema

DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
HOUR_MS = 60 * MINUTE_MS
# No trades for a whole hour on the second day, and for a few minutes on the first.
QUIET = [(DAY + DAY_MS + 5 * HOUR_MS, DAY + DAY_MS + 6 * HOUR_MS), (DAY + 90 * MINUTE_MS, DAY + 93 * MINUTE_MS)]


@pytest.fixture
def market():
    return SyntheticMarket(quiet=QUIET)


def traded_klines(market, symbol, start_ms, end_ms):
    return [k for k in market.klines_between(symbol, start_ms, end_ms) if k[8] > 0]


def test_every_traded_minute_is_read_oldest_first(tmp_path, symbol, market):
    write_klines(tmp_path, symbol, DAY, DAY + 3 * DAY_MS, market)
    minutes = read_minutes(str(tmp_path), symbol)
    expected = traded_klines(market, symbol, DAY, DAY + 3 * DAY_MS)
    assert len(minutes) == len(expected) == 3 * 1440 - 60 - 3  # zero-trade minutes left out, as candles_1m does
    assert minutes.start == [k[0] // 1000 for k in expected]
    assert minutes.close == [Decimal(k[4]) for k in expected]
    assert minutes.volume == [Decimal(k[5]) for k in expected]


def test_without_klines_there_is_no_history(tmp_path, symbol):
    with pytest.raises(NoHistory, match="run the backfill first"):
        read_minutes(str(tmp_path), symbol)


@pytest.fixture
def candle_store(postgres_schema, pg, symbol):
    store = CandleStore()
    try:
        yield store
    finally:
        store.close()
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))


@pytest.mark.parametrize("timeframe", ["1m", "5m", "1h", "4h"])
def test_bars_are_the_bars_the_chart_draws(tmp_path, symbol, market, candle_store, timeframe):
    for day in write_klines(tmp_path, symbol, DAY, DAY + 2 * DAY_MS + 150 * MINUTE_MS, market):
        candle_store.upsert_candles(symbol, day)  # what the backfill loads into candles_1m
    bars = build_bars(read_minutes(str(tmp_path), symbol), timeframe)
    reader = CandleHistoryReader()
    try:
        chart = reader.get_candles(symbol, limit=1440 * 3, timeframe=timeframe)
    finally:
        reader.close()
    chart_bars = [(int(c["window_start"].timestamp()), c["open"], c["high"], c["low"], c["close"], c["volume"]) for c in chart]
    # The chart also has the bar still forming at the end (on 4h: 02:00 to 04:00
    # on the third day, two and a half hours in); the backtest never trades it.
    assert [(b.time, b.open, b.high, b.low, b.close, b.volume) for b in bars] == chart_bars[: len(bars)]
    assert len(chart_bars) - len(bars) == (0 if timeframe in ("1m", "5m") else 1)


def test_the_bar_still_forming_where_the_data_ends_is_dropped(tmp_path, symbol):
    write_klines(tmp_path, symbol, DAY, DAY + 10 * HOUR_MS + 30 * MINUTE_MS)
    assert build_bars(read_minutes(str(tmp_path), symbol), "1h")[-1].time == (DAY + 9 * HOUR_MS) // 1000
    write_klines(tmp_path, symbol, DAY, DAY + 11 * HOUR_MS)  # 10:59 is in: the 10:00 bar is finished
    assert build_bars(read_minutes(str(tmp_path), symbol), "1h")[-1].time == (DAY + 10 * HOUR_MS) // 1000


def test_emas_are_the_browsers_on_every_bar_warmed_up_from_the_oldest_minute(tmp_path, symbol, market):
    write_klines(tmp_path, symbol, DAY, DAY + 10 * DAY_MS, market)
    minutes = read_minutes(str(tmp_path), symbol)
    bars = build_bars(minutes, "1h", ema_lengths=(9, 21))
    expected = math_js_ema(list(zip(minutes.start, map(str, minutes.close))), {"1h": 3600}, [9, 20, 21, 50, 200])["1h"]
    assert len(bars) == 10 * 24 - 1  # less the quiet hour
    for length in (9, 20, 21, 50, 200):
        assert [bar.ema[length] for bar in bars] == [expected[length][bar.time] for bar in bars], length
    assert bars[198].ema[200] is None and bars[199].ema[200] is not None  # seeded on the 200th bar


def test_a_bar_gets_its_footprint_only_where_aggtrades_cover_all_of_it(tmp_path, symbol, market):
    end = DAY + 2 * DAY_MS
    write_klines(tmp_path, symbol, DAY, end, market)
    covered_from = DAY + DAY_MS + 6 * HOUR_MS + 30 * MINUTE_MS  # half way through the 06:00 bar
    write_agg_trades(tmp_path, symbol, covered_from, end, market)
    bars = build_bars(read_minutes(str(tmp_path), symbol), "1h")
    footprints = read_footprint(str(tmp_path), symbol, 3600, bars[0].time, end // 1000)
    assert min(footprints) == (covered_from + 30 * MINUTE_MS) // 1000  # the 07:00 bar: the first whole one
    assert len(footprints) == 17

    shown = with_footprints(bars, footprints)
    for bar in shown:
        if bar.time not in footprints:
            assert bar.footprint is None
            continue
        levels = bar.footprint
        assert [price for price, _, _ in levels] == sorted({price for price, _, _ in levels})
        assert all(price == price.to_integral_value() for price, _, _ in levels)  # $1 levels
        assert sum(buy + sell for _, buy, sell in levels) == bar.volume


def test_footprint_levels_add_up_each_trade_by_side(tmp_path, symbol, market):
    start = DAY + 3 * HOUR_MS
    trades = write_agg_trades(tmp_path, symbol, start, start + HOUR_MS, market)
    (levels,) = read_footprint(str(tmp_path), symbol, 3600, start // 1000, (start + HOUR_MS) // 1000).values()
    expected = {}
    for t in trades:
        price = Decimal(t["p"]) // 1
        buy, sell = expected.get(price, (Decimal(0), Decimal(0)))
        quantity = Decimal(t["q"])
        expected[price] = (buy, sell + quantity) if t["m"] else (buy + quantity, sell)
    assert levels == tuple((price, *expected[price]) for price in sorted(expected))
