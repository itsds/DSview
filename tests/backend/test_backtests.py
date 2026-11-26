"""
Tests for /backtests (backend/routers/backtests.py + postgres_client.py's
BacktestReader): runs stored by backtest/run_backtest.py itself, over a
temporary Bronze, read back the way the results panel reads them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from config.settings import POSTGRES_DSN

import run_backtest

from bronze_fakes import DAY_MS, write_klines
from helpers import parse_utc

DAY = int(datetime(2026, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)


@pytest.fixture
def stored_runs(tmp_path, symbol, pg, postgres_schema):
    """Three runs of ema_cross over 6 days of klines: 1h, the same again, and 15m. Their run ids, oldest first."""
    write_klines(tmp_path, symbol, DAY, DAY + 6 * DAY_MS)
    for timeframe in ("1h", "1h", "15m"):
        args = ["--strategy", "ema_cross", "--symbol", symbol, "--timeframe", timeframe, "--no-check"]
        assert run_backtest.main(args, data_root=str(tmp_path), dsn=POSTGRES_DSN) == 0
    rows = pg.execute("SELECT run_id FROM backtest_runs WHERE symbol = %s ORDER BY run_id", (symbol,)).fetchall()
    yield [row["run_id"] for row in rows]
    pg.execute("DELETE FROM backtest_runs WHERE symbol = %s", (symbol,))


def test_runs_are_listed_newest_first_with_their_stats(client, stored_runs, symbol):
    runs = client.get("/backtests", params={"symbol": symbol}).json()
    assert [run["run_id"] for run in runs] == stored_runs[::-1]
    newest, rerun, first = runs
    assert (newest["timeframe"], newest["strategy"], newest["params"]) == ("15m", "ema_cross", {"fast": 20, "slow": 50})
    assert (rerun["same_as_run"], first["same_as_run"], newest["same_as_run"]) == (first["run_id"], None, None)
    assert (first["initial_capital"], first["fee_rate"]) == ("10000", "0.001")
    assert isinstance(first["stats"]["net_profit"], str) and first["stats"]["total_trades"] > 0
    assert parse_utc(first["first_bar"]) == datetime.fromtimestamp(DAY / 1000, timezone.utc)
    assert client.get("/backtests", params={"symbol": symbol, "limit": 1}).json()[0]["run_id"] == stored_runs[-1]
    assert client.get("/backtests", params={"symbol": "NOSUCHSYMBOL"}).json() == []


def test_a_run_comes_with_its_trades(client, stored_runs, pg):
    run = client.get(f"/backtests/{stored_runs[0]}").json()
    trades = run["trades"]
    assert len(trades) == run["stats"]["total_trades"]
    assert [t["trade_no"] for t in trades] == list(range(1, len(trades) + 1))
    stored = pg.execute("SELECT * FROM backtest_trades WHERE run_id = %s ORDER BY trade_no", (stored_runs[0],)).fetchall()
    for got, row in zip(trades, stored):
        assert got["side"] == row["side"] and parse_utc(got["entry_time"]) == row["entry_time"]
        assert Decimal(got["pnl"]) == row["pnl"] and Decimal(got["entry_price"]) == row["entry_price"]
        for text in (got["pnl"], got["entry_price"], got["quantity"]):  # plain, trimmed decimals
            assert "E" not in text and not ("." in text and text.endswith("0")), text
    # The trades' P&L add up to the net profit, to the last of broker.py's 28 digits.
    assert abs(sum(Decimal(t["pnl"]) for t in trades) - Decimal(run["stats"]["net_profit"])) < Decimal("1e-20")
    assert trades[-1]["closed_by_end"] in (True, False)


def test_an_unknown_run_is_404(client, postgres_schema):
    assert client.get("/backtests/999999999").status_code == 404
    assert client.get("/backtests/999999999/equity").status_code == 404


def test_the_equity_curve_in_full_when_it_is_short(client, stored_runs, pg):
    curve = client.get(f"/backtests/{stored_runs[0]}/equity").json()
    stored = pg.execute("SELECT * FROM backtest_equity WHERE run_id = %s ORDER BY window_start", (stored_runs[0],)).fetchall()
    assert curve["total"] == len(stored) == 6 * 24
    assert [(parse_utc(t), Decimal(e)) for t, e in curve["points"]] == [(r["window_start"], r["equity"]) for r in stored]


def test_a_long_curve_is_thinned_keeping_every_trough_and_peak(client, stored_runs, pg):
    run_id = stored_runs[2]  # 15m: 576 points
    stored = pg.execute("SELECT * FROM backtest_equity WHERE run_id = %s ORDER BY window_start", (run_id,)).fetchall()
    curve = client.get(f"/backtests/{run_id}/equity", params={"max_points": 100}).json()
    points = [(parse_utc(t), Decimal(e)) for t, e in curve["points"]]
    assert curve["total"] == len(stored) == 576
    assert 90 <= len(points) <= 102
    assert points == sorted(points)  # oldest first, no repeats
    assert points[0] == (stored[0]["window_start"], stored[0]["equity"])
    assert points[-1] == (stored[-1]["window_start"], stored[-1]["equity"])
    values = [e for _, e in points]
    assert min(values) == min(r["equity"] for r in stored) and max(values) == max(r["equity"] for r in stored)
    assert set(points) <= {(r["window_start"], r["equity"]) for r in stored}


def test_bad_parameters_are_422(client, postgres_schema):
    assert client.get("/backtests").status_code == 422  # symbol is required
    assert client.get("/backtests", params={"symbol": "X", "limit": 0}).status_code == 422
    assert client.get("/backtests/1/equity", params={"max_points": 1}).status_code == 422
