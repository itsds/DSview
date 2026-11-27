-- backtests.sql
--
-- Backtest results (Phase 4b): backtest/run_backtest.py writes one run per
-- invocation, backend/routers/backtests.py serves them to the chart's
-- results panel. Every run is kept (kickoff decision 1): a rerun is a new
-- run, and result_digest, a hash of its trades and equity curve, says
-- whether two runs came out the same: same_as_run is the newest earlier
-- run with the same settings and span and the same digest, if any.
--
-- Live's schema only (storage/init_db.py skips this file for the replay):
-- backtests read live's lake, and a replay has no results to keep.
--
-- Money is NUMERIC without a fixed scale: backtest/broker.py computes in
-- 28-digit decimals and every digit is kept, so what the panel shows is what
-- was computed. Times are bar starts, as the chart's bars are.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id           BIGSERIAL     PRIMARY KEY,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    symbol           VARCHAR(20)   NOT NULL,
    strategy         VARCHAR(64)   NOT NULL,
    params           JSONB         NOT NULL,   -- every parameter, defaults included
    timeframe        VARCHAR(4)    NOT NULL,
    first_bar        TIMESTAMPTZ   NOT NULL,   -- the first bar the strategy saw
    last_bar         TIMESTAMPTZ   NOT NULL,   -- and the last
    initial_capital  NUMERIC       NOT NULL,
    fee_rate         NUMERIC       NOT NULL,   -- per side, a fraction of the traded value
    stats            JSONB         NOT NULL,   -- backtest/backtest_stats.py's, decimals as strings
    spark_check      JSONB,                    -- backtest/indicator_check.py's, NULL if not checked
    result_digest    CHAR(64)      NOT NULL,   -- sha256 of the trades and the equity curve
    same_as_run      BIGINT        REFERENCES backtest_runs (run_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_run
    ON backtest_runs (symbol, run_id DESC);

CREATE TABLE IF NOT EXISTS backtest_trades (
    run_id         BIGINT       NOT NULL REFERENCES backtest_runs (run_id) ON DELETE CASCADE,
    trade_no       INTEGER      NOT NULL,
    side           VARCHAR(5)   NOT NULL,   -- long or short
    entry_time     TIMESTAMPTZ  NOT NULL,   -- the start of the bar whose open filled it
    entry_price    NUMERIC      NOT NULL,
    exit_time      TIMESTAMPTZ  NOT NULL,
    exit_price     NUMERIC      NOT NULL,
    quantity       NUMERIC      NOT NULL,
    fees           NUMERIC      NOT NULL,
    pnl            NUMERIC      NOT NULL,   -- net of fees
    return_pct     NUMERIC      NOT NULL,
    bars           INTEGER      NOT NULL,
    closed_by_end  BOOLEAN      NOT NULL,   -- still open when the data ended
    PRIMARY KEY (run_id, trade_no)
);

CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id        BIGINT       NOT NULL REFERENCES backtest_runs (run_id) ON DELETE CASCADE,
    window_start  TIMESTAMPTZ  NOT NULL,   -- the bar; equity at its close
    equity        NUMERIC      NOT NULL,
    PRIMARY KEY (run_id, window_start)
);
