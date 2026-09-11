-- gold_candles.sql
--
-- Serving-copy table for finalized OHLCV candles — the Gold layer's
-- queryable counterpart to storage/data/gold/candles_1m/ (the durable
-- Parquet copy written by streaming/jobs/storage_writer.py). This table
-- exists purely so the backend's REST historical-candles endpoint (not
-- yet built) can answer a request with an indexed SELECT instead of
-- spinning up Spark to scan Parquet.
--
-- Written by streaming/postgres_sink.py via upsert — see its module
-- docstring for why upserting on every batch (not just on window close)
-- is correct here.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol        VARCHAR(20)      NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    open          NUMERIC(38, 18)  NOT NULL,
    high          NUMERIC(38, 18)  NOT NULL,
    low           NUMERIC(38, 18)  NOT NULL,
    close         NUMERIC(38, 18)  NOT NULL,
    volume        NUMERIC(38, 18)  NOT NULL,
    trade_count   INTEGER          NOT NULL,
    PRIMARY KEY (symbol, window_start)
);

-- Backend queries will always be "most recent N candles for this symbol".
CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_window_start
    ON candles_1m (symbol, window_start DESC);
