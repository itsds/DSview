"""
Tests for backfill/history_backfill.py: a run over a fake Binance into the
test Postgres and a temporary Bronze, and what reruns, interruptions and
live candles do to it. The fake's market is a pure function of time, so
every expected candle and trade is computed here from the same source.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import history_backfill
from binance_rest import BinanceHistoryClient, BinanceUnavailable, WeightBudget
from bronze_files import AGG_TRADES, KLINES, coverage, day_path
from candle_store import CandleStore
from fake_binance import MINUTE_MS
from history_backfill import DAY_MS, HistoryBackfill, plan_days, settled_end

# Midnight UTC, comfortably in the past; the runs below end partway into DAY + 2.
DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
END = DAY + 2 * DAY_MS + 90 * MINUTE_MS


def utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def test_plan_days_covers_whole_days_back_from_the_end_newest_first():
    spans = plan_days(2, END)
    assert [s.day for s in spans] == [date(2026, 3, 4), date(2026, 3, 3), date(2026, 3, 2)]
    assert (spans[0].start_ms, spans[0].end_ms) == (DAY + 2 * DAY_MS, END)  # today: up to the end
    assert (spans[1].start_ms, spans[1].end_ms) == (DAY + DAY_MS, DAY + 2 * DAY_MS)
    assert plan_days(0, END) == []


def test_a_run_stops_at_the_minute_that_closed_two_minutes_ago():
    now = (DAY + 10 * MINUTE_MS + 25_000) / 1000  # 00:10:25
    assert settled_end(now) == DAY + 8 * MINUTE_MS  # 00:07 is the last minute in


@pytest.fixture
def store(postgres_schema):
    store = CandleStore()
    try:
        yield store
    finally:
        store.close()


@pytest.fixture
def run(fake_binance, fake_time, store, tmp_path):
    """Run a backfill (returning its results) against the fake, Postgres and tmp_path's Bronze."""

    async def run_backfill(symbol, candle_days=2, trade_days=1, end_ms=END, rebuild=False, max_attempts=8):
        budget = WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep)
        async with BinanceHistoryClient(
            fake_binance.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock, max_attempts=max_attempts
        ) as client:
            backfill = HistoryBackfill(client, store, data_root=str(tmp_path))
            return (await backfill.run([symbol], candle_days, trade_days, end_ms, rebuild=rebuild))[symbol]

    return run_backfill


def stored_candles(pg, symbol):
    return pg.execute("SELECT * FROM candles_1m WHERE symbol = %s ORDER BY window_start", (symbol,)).fetchall()


def revision(pg, symbol):
    row = pg.execute("SELECT revision FROM candle_revisions WHERE symbol = %s", (symbol,)).fetchone()
    return row["revision"] if row else 0


def expected_klines(fake, symbol, start_ms, end_ms):
    return list(fake.market.klines_between(symbol, start_ms, end_ms))


async def test_a_run_fills_candles_1m_and_both_bronze_datasets(run, fake_binance, pg, symbol, tmp_path):
    start = DAY  # plan_days(2, END) starts at DAY's midnight
    result = await run(symbol)

    klines = expected_klines(fake_binance, symbol, start, END)
    rows = stored_candles(pg, symbol)
    assert len(rows) == len(klines) == 2 * 1440 + 90
    for row, kline in zip(rows, klines):
        assert row["window_start"] == utc(kline[0])
        assert row["window_end"] == utc(kline[0] + MINUTE_MS)
        assert [row["open"], row["high"], row["low"], row["close"], row["volume"]] == [Decimal(v) for v in kline[1:6]]
        assert row["trade_count"] == kline[8]

    # Bronze klines: one file per day, covering exactly its span, every column.
    for span in plan_days(2, END):
        path = day_path(str(tmp_path), KLINES, symbol, span.day)
        assert coverage(path) == (span.start_ms, span.end_ms)
        table = pq.read_table(path).to_pylist()
        day_klines = expected_klines(fake_binance, symbol, span.start_ms, span.end_ms)
        assert [r["window_start"] for r in table] == [utc(k[0]) for k in day_klines]
        assert [str(r["taker_buy_volume"].normalize()) for r in table] == [str(Decimal(k[9]).normalize()) for k in day_klines]
        assert {r["exchange"] for r in table} == {"binance"}

    # Bronze aggTrades: one day back from END means DAY + 1 and DAY + 2.
    trades = []
    for span in plan_days(1, END):
        path = day_path(str(tmp_path), AGG_TRADES, symbol, span.day)
        assert coverage(path) == (span.start_ms, span.end_ms)
        trades += pq.read_table(path).to_pylist()
    expected = list(fake_binance.market.trades_between(symbol, DAY + DAY_MS, END))
    assert sorted(t["agg_trade_id"] for t in trades) == [t["a"] for t in expected]
    by_id = {t["agg_trade_id"]: t for t in trades}
    for trade in expected[:50]:
        stored = by_id[trade["a"]]
        assert stored["event_time"] == utc(trade["T"])
        assert stored["price"] == Decimal(trade["p"]) and stored["quantity"] == Decimal(trade["q"])
        assert stored["side"] == ("sell" if trade["m"] else "buy")
        assert (stored["first_trade_id"], stored["last_trade_id"]) == (trade["f"], trade["l"])

    assert result.candles_changed == len(klines)
    assert result.oldest_change == utc(start)
    assert result.trades == len(expected)
    assert revision(pg, symbol) == result.revision == 1
    # No temporary file is left behind anywhere.
    assert not [p for p in Path(tmp_path).rglob(".*") if p.is_file()]


async def test_a_rerun_fetches_nothing_it_has_and_changes_nothing(run, fake_binance, pg, symbol):
    await run(symbol)
    before = stored_candles(pg, symbol)
    fake_binance.requests.clear()

    result = await run(symbol)

    assert stored_candles(pg, symbol) == before  # no duplicates, nothing rewritten
    assert fake_binance.requests == []
    assert (result.candle_days_fetched, result.candle_days_reloaded) == (0, 3)
    assert (result.trade_days_fetched, result.trade_days_skipped) == (0, 2)
    assert result.candles_changed == 0 and result.footprint_changed == 0
    assert result.revision is None and revision(pg, symbol) == 1  # nothing to rebuild


async def test_a_later_run_refetches_only_today_and_asks_for_a_rebuild(run, fake_binance, pg, symbol):
    await run(symbol)
    fake_binance.requests.clear()

    result = await run(symbol, end_ms=END + 30 * MINUTE_MS)

    today = DAY + 2 * DAY_MS
    assert [int(r["startTime"]) for r in fake_binance.requests_to("/api/v3/klines")] == [today]
    trade_requests = fake_binance.requests_to("/api/v3/aggTrades")
    assert int(trade_requests[0]["startTime"]) == today and all("fromId" in r for r in trade_requests[1:])
    assert (result.candle_days_fetched, result.trade_days_fetched, result.trade_days_skipped) == (1, 1, 1)
    assert len(stored_candles(pg, symbol)) == 2 * 1440 + 120
    assert result.candles_changed == 30
    assert result.oldest_change == utc(END)
    assert revision(pg, symbol) == 2


async def test_the_exchanges_candle_replaces_a_short_live_one(run, fake_binance, pg, symbol):
    minute = DAY + 2 * DAY_MS + 30 * MINUTE_MS
    kline = fake_binance.market.kline(symbol, minute)
    same_minute = minute + MINUTE_MS
    same = fake_binance.market.kline(symbol, same_minute)
    insert = """
        INSERT INTO candles_1m (symbol, window_start, window_end, open, high, low, close, volume, trade_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Live missed this minute's last trades...
    pg.execute(insert, (symbol, utc(minute), utc(minute + MINUTE_MS), kline[1], kline[2], kline[3], kline[1], "0.001", 1))
    # ...and got this one complete, written without Binance's trailing zeros.
    pg.execute(insert, (symbol, utc(same_minute), utc(same_minute + MINUTE_MS),
                        *[str(Decimal(v).normalize()) for v in same[1:6]], same[8]))

    result = await run(symbol, candle_days=1, trade_days=0)

    rows = {row["window_start"]: row for row in stored_candles(pg, symbol)}
    assert rows[utc(minute)]["close"] == Decimal(kline[4]) and rows[utc(minute)]["volume"] == Decimal(kline[5])
    assert result.candles_changed == 1440 + 90 - 1  # everything but the minute live already had right


def stored_footprint(pg, symbol):
    return pg.execute(
        "SELECT window_start, price, buy_volume, sell_volume FROM footprint_1m WHERE symbol = %s ORDER BY window_start, price",
        (symbol,),
    ).fetchall()


async def test_each_trade_day_fills_the_footprint_and_every_minute_adds_up_to_its_candle(run, fake_binance, pg, symbol):
    result = await run(symbol, candle_days=2, trade_days=1)

    rows = stored_footprint(pg, symbol)
    assert result.footprint_changed == len(rows) > 0
    by_minute = {}
    for row in rows:
        by_minute[row["window_start"]] = by_minute.get(row["window_start"], 0) + row["buy_volume"] + row["sell_volume"]
    candles = {row["window_start"]: row["volume"] for row in stored_candles(pg, symbol)}
    # The trade days (DAY + 1 and DAY + 2) have a footprint; the candle-only day doesn't.
    trade_minutes = {utc(kline[0]) for kline in expected_klines(fake_binance, symbol, DAY + DAY_MS, END) if kline[8] > 0}
    assert set(by_minute) == trade_minutes
    for minute, volume in by_minute.items():
        assert volume == candles[minute]
    for row in rows:
        assert row["price"] == row["price"].to_integral_value()  # $1 levels

    # Each level against the fake's own trades.
    minute = DAY + DAY_MS + 600 * MINUTE_MS
    expected = {}
    for trade in fake_binance.market.trades_in_minute(symbol, minute):
        level = Decimal(trade["p"]) // 1
        buy, sell = expected.get(level, (Decimal(0), Decimal(0)))
        quantity = Decimal(trade["q"])
        expected[level] = (buy, sell + quantity) if trade["m"] else (buy + quantity, sell)
    got = {row["price"]: (row["buy_volume"], row["sell_volume"]) for row in rows if row["window_start"] == utc(minute)}
    assert got == expected


async def test_a_rerun_leaves_the_footprint_alone_and_live_rows_are_replaced(run, fake_binance, pg, symbol):
    await run(symbol, candle_days=0, trade_days=1)
    before = stored_footprint(pg, symbol)

    result = await run(symbol, candle_days=0, trade_days=1)
    assert result.footprint_changed == 0 and stored_footprint(pg, symbol) == before

    # What live wrote for a closed minute gives way to the exchange's trades:
    # a short level is corrected, a level the exchange never traded at goes.
    # Live minutes after the run's end are left alone.
    minute = utc(DAY + DAY_MS + 30 * MINUTE_MS)
    level = next(row for row in before if row["window_start"] == minute)
    pg.execute(
        "UPDATE footprint_1m SET buy_volume = 0.001 WHERE symbol = %s AND window_start = %s AND price = %s",
        (symbol, minute, level["price"]),
    )
    insert = "INSERT INTO footprint_1m VALUES (%s, %s, %s, 1, 1)"
    pg.execute(insert, (symbol, minute, Decimal("1")))
    pg.execute(insert, (symbol, utc(END), Decimal("1")))

    result = await run(symbol, candle_days=0, trade_days=1)
    assert result.footprint_changed == 2
    assert [r for r in stored_footprint(pg, symbol) if r["window_start"] < utc(END)] == before
    assert [r["window_start"] for r in stored_footprint(pg, symbol) if r["window_start"] >= utc(END)] == [utc(END)]


async def test_minutes_without_trades_stay_out_of_candles_1m_but_in_bronze(run, fake_binance, pg, symbol, tmp_path):
    today = DAY + 2 * DAY_MS
    fake_binance.market.quiet = [(today + 10 * MINUTE_MS, today + 15 * MINUTE_MS)]
    fake_binance.market.outages = [(today + 20 * MINUTE_MS, today + 30 * MINUTE_MS)]
    await run(symbol, candle_days=1, trade_days=0)

    starts = {row["window_start"] for row in stored_candles(pg, symbol)}
    bronze = pq.read_table(day_path(str(tmp_path), KLINES, symbol, date(2026, 3, 4))).to_pylist()
    bronze_starts = {r["window_start"]: r for r in bronze}
    for m in range(10, 15):
        assert utc(today + m * MINUTE_MS) not in starts
        assert bronze_starts[utc(today + m * MINUTE_MS)]["trade_count"] == 0
    for m in range(20, 30):
        assert utc(today + m * MINUTE_MS) not in starts and utc(today + m * MINUTE_MS) not in bronze_starts
    assert len(bronze) == 90 - 10


async def test_a_failed_run_keeps_finished_days_and_leaves_no_partial_file(run, fake_binance, pg, symbol, tmp_path):
    await run(symbol, candle_days=2, trade_days=0, end_ms=END - 30 * MINUTE_MS)
    fake_binance.fail_next(503, count=1000)

    with pytest.raises(BinanceUnavailable):
        await run(symbol, candle_days=2, trade_days=1, max_attempts=2)

    # Today's new minutes needed Binance, which is down; the half-fetched
    # trade days left no file, not even a temporary one.
    assert not [p for p in Path(tmp_path).rglob("*") if p.is_file() and "agg_trades" in str(p)]
    assert not [p for p in Path(tmp_path).rglob(".*") if p.is_file()]
    assert len(stored_candles(pg, symbol)) == 2 * 1440 + 60
    assert revision(pg, symbol) == 1  # nothing changed: nothing to rebuild

    fake_binance.failures.clear()
    result = await run(symbol, candle_days=2, trade_days=1)
    assert len(stored_candles(pg, symbol)) == 2 * 1440 + 90
    assert result.candles_changed == 30 and revision(pg, symbol) == 2


async def test_a_run_that_fails_after_changing_candles_still_asks_for_a_rebuild(
    fake_binance, fake_time, store, pg, symbol, tmp_path
):
    # The oldest candle day fails; the two newer ones (fetched first, one at a time) are in.
    fake_binance.fail_matching(lambda path, params: path == "/api/v3/klines" and int(params["startTime"]) < DAY + DAY_MS, 503)
    budget = WeightBudget(wall_clock=fake_time.clock, sleep=fake_time.sleep)
    async with BinanceHistoryClient(
        fake_binance.url, budget=budget, sleep=fake_time.sleep, clock=fake_time.clock, max_attempts=2
    ) as client:
        backfill = HistoryBackfill(client, store, data_root=str(tmp_path), candle_concurrency=1)
        with pytest.raises(BinanceUnavailable):
            await backfill.run([symbol], 2, 0, END)

    assert len(stored_candles(pg, symbol)) == 1440 + 90
    assert revision(pg, symbol) == 1


async def test_rebuild_alone_bumps_the_revision_without_fetching(run, fake_binance, pg, symbol):
    result = await run(symbol, candle_days=0, trade_days=0, rebuild=True)
    assert result.revision == 1 and revision(pg, symbol) == 1
    assert fake_binance.requests == []


def test_main_runs_the_cli_end_to_end(fake_binance, pg, symbol, tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(history_backfill, "BINANCE_REST_URL", fake_binance.url)
    monkeypatch.setattr(history_backfill, "DATA_ROOT", str(tmp_path))
    until = utc(DAY + 2 * MINUTE_MS + 2 * MINUTE_MS + 30_000).isoformat()  # stops at 00:02

    with caplog.at_level(logging.INFO):
        assert history_backfill.main(["--symbol", symbol, "--days", "1", "--trade-days", "1", "--until", until]) == 0
    assert len(stored_candles(pg, symbol)) == 1440 + 2
    assert day_path(str(tmp_path), AGG_TRADES, symbol, date(2026, 3, 2)).exists()
    assert "indicator rebuild requested (revision 1)" in caplog.text

    assert history_backfill.main(["--symbol", symbol, "--rebuild-indicators-only"]) == 0
    assert revision(pg, symbol) == 2


def test_main_explains_an_unknown_symbol(fake_binance, tmp_path, monkeypatch, caplog, postgres_schema):
    monkeypatch.setattr(history_backfill, "BINANCE_REST_URL", fake_binance.url)
    monkeypatch.setattr(history_backfill, "DATA_ROOT", str(tmp_path))
    assert history_backfill.main(["--symbol", "NOPE", "--days", "1", "--trade-days", "0", "--until", utc(END).isoformat()]) == 1
    assert "Invalid symbol" in caplog.text
