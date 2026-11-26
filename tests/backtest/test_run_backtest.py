"""
Tests for backtest/run_backtest.py and backtest_store.py: a backtest from
the command line over a temporary Bronze (tests/bronze_fakes.py) into the
test Postgres: what a run stores, reruns, the check against Spark's
candle_indicators, and the footprint for a strategy that asks for it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from config.settings import POSTGRES_DSN, SERVER_EMA_LENGTHS

import run_backtest
from backtest_stats import compute_stats
from backtester import run
from bar_series import build_bars
from lake_history import read_minutes
from strategies import STRATEGIES
from strategies.ema_cross import EmaCross
from strategy import Strategy

from bronze_fakes import DAY_MS, write_agg_trades, write_klines

DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
DAYS = 12


@pytest.fixture
def lake(tmp_path, symbol, pg):
    write_klines(tmp_path, symbol, DAY, DAY + DAYS * DAY_MS)
    yield tmp_path
    pg.execute("DELETE FROM backtest_runs WHERE symbol = %s", (symbol,))
    pg.execute("DELETE FROM candle_indicators WHERE symbol = %s", (symbol,))


def backtest(lake, symbol, *args):
    return run_backtest.main(["--strategy", "ema_cross", "--symbol", symbol, *args], data_root=str(lake), dsn=POSTGRES_DSN)


def runs(pg, symbol):
    return pg.execute("SELECT * FROM backtest_runs WHERE symbol = %s ORDER BY run_id", (symbol,)).fetchall()


def test_a_run_is_stored_with_its_trades_equity_and_stats(lake, symbol, pg, capsys):
    assert backtest(lake, symbol, "--no-check") == 0
    (stored,) = runs(pg, symbol)
    expected = run(EmaCross(), build_bars(read_minutes(str(lake), symbol), "1h", (20, 50)))
    stats = compute_stats(expected)

    assert (stored["strategy"], stored["params"], stored["timeframe"]) == ("ema_cross", {"fast": 20, "slow": 50}, "1h")
    assert stored["first_bar"] == datetime.fromtimestamp(expected.bars[0].time, timezone.utc)
    assert stored["last_bar"] == datetime.fromtimestamp(expected.bars[-1].time, timezone.utc)
    assert (stored["initial_capital"], stored["fee_rate"]) == (Decimal(10000), Decimal("0.001"))
    assert stored["spark_check"] is None
    assert stored["stats"]["total_trades"] == len(expected.trades) > 10
    assert Decimal(stored["stats"]["net_profit"]) == stats["net_profit"]

    trades = pg.execute("SELECT * FROM backtest_trades WHERE run_id = %s ORDER BY trade_no", (stored["run_id"],)).fetchall()
    assert [(t["side"], t["entry_price"], t["exit_price"], t["pnl"], t["closed_by_end"]) for t in trades] == [
        (t.side, t.entry_price, t.exit_price, t.pnl, t.closed_by_end) for t in expected.trades
    ]
    equity = pg.execute("SELECT window_start, equity FROM backtest_equity WHERE run_id = %s ORDER BY window_start",
                        (stored["run_id"],)).fetchall()
    assert [(int(e["window_start"].timestamp()), e["equity"]) for e in equity] == expected.equity
    assert f"stored as run {stored['run_id']}" in capsys.readouterr().out


def test_a_rerun_is_a_new_run_with_the_same_result(lake, symbol, pg, capsys):
    backtest(lake, symbol, "--no-check")
    backtest(lake, symbol, "--no-check")
    first, second = runs(pg, symbol)
    assert first["result_digest"] == second["result_digest"]
    assert (first["same_as_run"], second["same_as_run"]) == (None, first["run_id"])
    assert f"the same result as run {first['run_id']}" in capsys.readouterr().out
    backtest(lake, symbol, "--no-check", "--param", "fast=10")  # other settings: no match
    assert "the same result" not in capsys.readouterr().out


def spark_rows(pg, symbol, bars):
    """candle_indicators rows with exactly the bars' server EMAs, as Spark would have written them."""
    for bar in bars:
        pg.execute(
            "INSERT INTO candle_indicators (symbol, timeframe, window_start, ema_20, ema_50, ema_200) VALUES (%s, '1h', %s, %s, %s, %s)",
            (symbol, datetime.fromtimestamp(bar.time, timezone.utc), *(bar.ema[length] for length in SERVER_EMA_LENGTHS)),
        )


def test_the_check_compares_every_bar_with_spark(lake, symbol, pg, capsys):
    bars = build_bars(read_minutes(str(lake), symbol), "1h", (20, 50))
    spark_rows(pg, symbol, bars[:-1])  # Spark hasn't reached the newest bar
    assert backtest(lake, symbol) == 0
    (stored,) = runs(pg, symbol)
    assert stored["spark_check"] == {"bars": len(bars), "equal": len(bars) - 1, "differ": 0, "missing": 1, "first_difference": None}
    assert f"{len(bars) - 1:,} of {len(bars):,} bars equal" in capsys.readouterr().out

    changed = bars[250]
    pg.execute(
        "UPDATE candle_indicators SET ema_200 = ema_200 + 1e-9 WHERE symbol = %s AND window_start = %s",
        (symbol, datetime.fromtimestamp(changed.time, timezone.utc)),
    )
    assert backtest(lake, symbol) == 1  # a difference fails the run, which is still stored
    check = runs(pg, symbol)[-1]["spark_check"]
    assert (check["differ"], check["first_difference"]["time"]) == (1, changed.time)
    assert "DIFFER" in capsys.readouterr().out


def test_start_and_end_bound_the_bars_traded(lake, symbol, pg):
    start, end = DAY + 3 * DAY_MS, DAY + 8 * DAY_MS
    iso = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()  # noqa: E731
    assert backtest(lake, symbol, "--no-check", "--timeframe", "4h", "--start", iso(start), "--end", iso(end)) == 0
    (stored,) = runs(pg, symbol)
    assert stored["timeframe"] == "4h"
    assert stored["first_bar"] == datetime.fromtimestamp(start / 1000, timezone.utc)
    assert stored["last_bar"] == datetime.fromtimestamp(end / 1000 - 4 * 3600, timezone.utc)  # the last bar ending by `end`
    assert stored["stats"]["bars"] == 5 * 6


def test_without_history_it_says_to_run_the_backfill(tmp_path, symbol, capsys):
    assert run_backtest.main(["--strategy", "ema_cross", "--symbol", symbol], data_root=str(tmp_path), dsn=POSTGRES_DSN) == 2
    assert "run the backfill first" in capsys.readouterr().err


class FootprintProbe(Strategy):
    name = "footprint_probe"
    needs_footprint = True
    seen = []

    def on_bar(self, bar, orders):
        FootprintProbe.seen.append(bar)


def test_a_strategy_that_asks_gets_each_covered_bars_footprint(lake, symbol, monkeypatch):
    monkeypatch.setitem(STRATEGIES, FootprintProbe.name, FootprintProbe)
    monkeypatch.setattr(FootprintProbe, "seen", [])
    covered = DAY + (DAYS - 2) * DAY_MS
    write_agg_trades(lake, symbol, covered, DAY + DAYS * DAY_MS)
    args = ["--strategy", "footprint_probe", "--symbol", symbol, "--no-save", "--no-check"]
    assert run_backtest.main(args, data_root=str(lake), dsn=POSTGRES_DSN) == 0
    with_footprint = [bar for bar in FootprintProbe.seen if bar.footprint is not None]
    assert [bar.time for bar in with_footprint] == [bar.time for bar in FootprintProbe.seen if bar.time >= covered // 1000]
    assert len(with_footprint) == 2 * 24
    assert all(sum(buy + sell for _, buy, sell in bar.footprint) == bar.volume for bar in with_footprint)


def test_the_backtest_tables_are_live_only(pg):
    assert pg.execute("SELECT to_regclass('public.backtest_runs') AS t").fetchone()["t"] is not None
    assert pg.execute("SELECT to_regclass('replay.backtest_runs') AS t").fetchone()["t"] is None
    assert pg.execute("SELECT to_regclass('replay.candles_1m') AS t").fetchone()["t"] is not None
