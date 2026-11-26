"""
backtests.py

The backtest results' endpoints (Phase 4b), for the chart's results panel
and its trade markers: runs that backtest/run_backtest.py stored in live's
Postgres (storage/ddl/backtests.sql), read through postgres_client.py's
BacktestReader. Live only: backtests read live's lake, so there is no
`?pipeline=`.

- `GET /backtests?symbol=BTCUSDT&limit=50`: the symbol's runs, newest
  first. Every run is kept (kickoff decision 1); each comes with its
  settings, span, stats and Spark check:

      [{"run_id": 12, "created_at": "...", "symbol": "BTCUSDT",
        "strategy": "ema_cross", "params": {"fast": 20, "slow": 50},
        "timeframe": "1h", "first_bar": "...", "last_bar": "...",
        "initial_capital": "10000", "fee_rate": "0.001",
        "stats": {"net_profit": "1234.5", "win_rate_pct": "38.2", ...},
        "spark_check": {"bars": 8760, "equal": 8760, "differ": 0, ...},
        "same_as_run": 11}, ...]

- `GET /backtests/{run_id}`: one run, as above, plus its `trades`, oldest
  first: {"trade_no", "side", "entry_time", "entry_price", "exit_time",
  "exit_price", "quantity", "fees", "pnl", "return_pct", "bars",
  "closed_by_end"}. Times are the starts of the bars whose open filled
  each side (a trade still open when the data ended exits at its last
  bar's close). 404 for an unknown run.

- `GET /backtests/{run_id}/equity?max_points=2000`: the equity curve,
  marked at every bar's close, thinned to about `max_points` points
  keeping every trough and peak: {"points": [[time, equity], ...],
  "total": the full curve's length}.

Numbers are plain decimal strings throughout, as the footprint's are.

Author: @DS
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from config.settings import LIVE_PIPELINE

from postgres_client import BacktestReader

router = APIRouter()

DEFAULT_RUNS = 50
MAX_RUNS = 500
DEFAULT_POINTS = 2000
MAX_POINTS = 20000


def _reader() -> BacktestReader:
    return BacktestReader(dsn=LIVE_PIPELINE.postgres_dsn)


@router.get("/backtests")
def list_backtests(symbol: str, limit: int = Query(DEFAULT_RUNS, ge=1, le=MAX_RUNS)) -> list[dict]:
    reader = _reader()
    try:
        return reader.list_runs(symbol, limit)
    finally:
        reader.close()


@router.get("/backtests/{run_id}")
def get_backtest(run_id: int) -> dict:
    reader = _reader()
    try:
        run = reader.get_run(run_id)
    finally:
        reader.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"no backtest run {run_id}")
    return run


@router.get("/backtests/{run_id}/equity")
def get_backtest_equity(run_id: int, max_points: int = Query(DEFAULT_POINTS, ge=2, le=MAX_POINTS)) -> dict:
    reader = _reader()
    try:
        if not reader.run_exists(run_id):
            raise HTTPException(status_code=404, detail=f"no backtest run {run_id}")
        return reader.get_equity(run_id, max_points)
    finally:
        reader.close()
