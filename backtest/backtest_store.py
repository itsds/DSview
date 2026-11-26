"""
backtest_store.py

The one place backtest/ touches Postgres: writes each run's results into
live's `backtest_runs`, `backtest_trades` and `backtest_equity`
(storage/ddl/backtests.sql), which backend/routers/backtests.py serves to
the chart, and reads Spark's `candle_indicators` for indicator_check.py.

**Every run is kept** (Phase 4b's kickoff decision 1), in one transaction:
the run, its trades and its equity curve together or not at all. The trades
and the curve go in through COPY (a year of 1m bars is 525,600 points).

**Same result?** `result_digest()` hashes a run's trades and curve (every
digit, as stored), and `save()` records in the run's `same_as_run` the
newest earlier run with the same strategy, parameters, timeframe, span,
capital and fee that came out identical: how "a rerun gives the same
result" is seen, here and in the results panel.

It refuses to start, with the fix in the message, if the tables are
missing (a database set up before Phase 4b).

Author: @DS
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Tuple

import psycopg
from psycopg.types.json import Jsonb

from config.settings import LIVE_PIPELINE, SERVER_EMA_LENGTHS

from backtest_stats import to_json
from backtester import BacktestResult
from indicator_check import SparkRows

TABLES = ("backtest_runs", "backtest_trades", "backtest_equity")

# The newest earlier run with the run's settings and span that came out identical.
_SAME_AS_SQL = """
    SELECT earlier.run_id
    FROM backtest_runs AS run
    JOIN backtest_runs AS earlier
      ON earlier.run_id < run.run_id
     AND earlier.symbol = run.symbol AND earlier.strategy = run.strategy
     AND earlier.params = run.params AND earlier.timeframe = run.timeframe
     AND earlier.first_bar = run.first_bar AND earlier.last_bar = run.last_bar
     AND earlier.initial_capital = run.initial_capital AND earlier.fee_rate = run.fee_rate
     AND earlier.result_digest = run.result_digest
    WHERE run.run_id = %s
    ORDER BY earlier.run_id DESC
    LIMIT 1
"""

_TRADE_COLUMNS = (
    "run_id", "trade_no", "side", "entry_time", "entry_price", "exit_time", "exit_price",
    "quantity", "fees", "pnl", "return_pct", "bars", "closed_by_end",
)


class MissingTables(RuntimeError):
    pass


def result_digest(result: BacktestResult) -> str:
    """sha256 of the trades and the equity curve, every digit included."""
    digest = hashlib.sha256()
    for t in result.trades:
        digest.update(
            f"T|{t.number}|{t.side}|{t.entry_time}|{t.entry_price}|{t.exit_time}|{t.exit_price}|"
            f"{t.quantity}|{t.fees}|{t.pnl}|{t.return_pct}|{t.bars}|{t.closed_by_end}\n".encode()
        )
    for time, equity in result.equity:
        digest.update(f"E|{time}|{equity}\n".encode())
    return digest.hexdigest()


def _utc(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


class BacktestStore:
    def __init__(self, dsn: str = LIVE_PIPELINE.postgres_dsn) -> None:
        self._conn = psycopg.connect(dsn)
        missing = [t for t in TABLES if self._conn.execute("SELECT to_regclass(%s)", (t,)).fetchone()[0] is None]
        self._conn.rollback()
        if missing:
            self._conn.close()
            raise MissingTables(
                f"Postgres has no {', '.join(missing)}: run `python storage/init_db.py` from the repo root first"
            )

    def close(self) -> None:
        self._conn.close()

    def save(
        self,
        symbol: str,
        strategy: str,
        params: Mapping[str, Any],
        timeframe: str,
        result: BacktestResult,
        stats: Mapping[str, Any],
        spark_check: Optional[Mapping[str, Any]],
        digest: str,
    ) -> Tuple[int, Optional[int]]:
        """Store one run; returns its run_id and the earlier run it came out the same as, if any."""
        if not result.bars:
            raise ValueError("a run without bars has nothing to store")
        with self._conn.transaction():
            run_id = self._conn.execute(
                """
                INSERT INTO backtest_runs (symbol, strategy, params, timeframe, first_bar, last_bar,
                                           initial_capital, fee_rate, stats, spark_check, result_digest)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
                """,
                (
                    symbol, strategy, Jsonb(dict(params)), timeframe,
                    _utc(result.bars[0].time), _utc(result.bars[-1].time),
                    result.capital, result.fee_rate, Jsonb(to_json(dict(stats))),
                    None if spark_check is None else Jsonb(json.loads(json.dumps(spark_check))),
                    digest,
                ),
            ).fetchone()[0]
            same_as = self._conn.execute(_SAME_AS_SQL, (run_id,)).fetchone()
            same_as = None if same_as is None else same_as[0]
            if same_as is not None:
                self._conn.execute("UPDATE backtest_runs SET same_as_run = %s WHERE run_id = %s", (same_as, run_id))
            with self._conn.cursor() as cur:
                with cur.copy(f"COPY backtest_trades ({', '.join(_TRADE_COLUMNS)}) FROM STDIN") as copy:
                    for t in result.trades:
                        copy.write_row((
                            run_id, t.number, t.side, _utc(t.entry_time), t.entry_price, _utc(t.exit_time),
                            t.exit_price, t.quantity, t.fees, t.pnl, t.return_pct, t.bars, t.closed_by_end,
                        ))
                with cur.copy("COPY backtest_equity (run_id, window_start, equity) FROM STDIN") as copy:
                    for time, equity in result.equity:
                        copy.write_row((run_id, _utc(time), equity))
        return run_id, same_as

    def spark_emas(self, symbol: str, timeframe: str, first_bar: int, last_bar: int) -> SparkRows:
        """Spark's candle_indicators rows for the bars from first_bar to last_bar (Unix seconds), by bar start."""
        columns = ", ".join(f"ema_{length}" for length in SERVER_EMA_LENGTHS)
        rows = self._conn.execute(
            f"""
            SELECT extract(epoch FROM window_start)::bigint, {columns}
            FROM candle_indicators
            WHERE symbol = %s AND timeframe = %s AND window_start BETWEEN %s AND %s
            """,
            (symbol, timeframe, _utc(first_bar), _utc(last_bar)),
        ).fetchall()
        self._conn.rollback()
        return {row[0]: dict(zip(SERVER_EMA_LENGTHS, row[1:])) for row in rows}
