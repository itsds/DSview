-- futures_ratios.sql
--
-- Binance Futures' long/short ratios (Phase 4c), for each symbol's USDⓈ-M
-- perpetual: one row per symbol, 5-minute period and ratio (`kind`, one of
-- config/settings.py's FUTURES_RATIO_KINDS), as Binance reports it.
-- period_start is the period's start, as candles_1m's window_start is.
--
--   global_accounts, top_accounts, top_positions: ratio = longs / shorts,
--     long_value and short_value the shares long and short (0 to 1) of all
--     accounts, of the top traders' accounts, of the top traders' positions;
--   taker_volume: ratio = taker buy / sell volume, long_value and
--     short_value that buy and sell volume in the base asset.
--
-- Stored at Binance's finest period only and rolled up at read time, the
-- way candles_1m is (backend/postgres_client.py's FuturesRatioReader).
-- Binance keeps only the latest 30 days; this table keeps everything the
-- poller ever fetched, so its history outgrows Binance's.
--
-- One writer: streaming/jobs/futures_ratios.py, upserting what
-- ingestion/futures_main.py publishes to market.futures.ratios, a row
-- rewritten only when a value differs. Live's schema only (init_db.py's
-- LIVE_ONLY): the replay has no Futures feed.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS futures_ratios (
    symbol        VARCHAR(20)      NOT NULL,
    period_start  TIMESTAMPTZ      NOT NULL,
    kind          VARCHAR(20)      NOT NULL,
    ratio         NUMERIC(38, 18)  NOT NULL,
    long_value    NUMERIC(38, 18)  NOT NULL,
    short_value   NUMERIC(38, 18)  NOT NULL,
    -- Time first: every read is "this symbol, these periods", all four ratios.
    PRIMARY KEY (symbol, period_start, kind)
);
