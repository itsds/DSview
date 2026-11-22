-- footprint.sql
--
-- The volume footprint (Phase 3b): buy and sell volume per price level
-- within each 1m candle. A level is a FOOTPRINT_PRICE_STEP-wide price band
-- ($1 by default, config/settings.py) named by its lower bound, so a trade
-- at 97,000.40 adds to the 97,000 row. Buy and sell are the taker's side,
-- as ingestion normalizes every trade: buy volume lifted the ask, sell
-- volume hit the bid.
--
-- Stored once at the finest step and combined at read time, the way
-- candles_1m is for timeframes: backend/postgres_client.py merges levels
-- into coarser rows ($5, $10, ...) and minutes into bigger bars, and sums
-- a time range into a volume profile. A minute's buy + sell over all its
-- levels equals that minute's candle volume.
--
-- Two writers: streaming/jobs/candle_aggregator.py's live footprint query
-- upserts every level a micro-batch changed (streaming/postgres_sink.py),
-- and backfill/history_backfill.py loads the exchange's aggTrades for
-- closed minutes (backfill/candle_store.py), replacing what live wrote for
-- those minutes, as the backfill does with candles.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS footprint_1m (
    symbol        VARCHAR(20)      NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    price         NUMERIC(38, 18)  NOT NULL,
    buy_volume    NUMERIC(38, 18)  NOT NULL,
    sell_volume   NUMERIC(38, 18)  NOT NULL,
    -- Range scans by time, levels in price order within each minute: every
    -- read is "this symbol, these minutes".
    PRIMARY KEY (symbol, window_start, price)
);
