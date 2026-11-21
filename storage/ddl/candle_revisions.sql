-- candle_revisions.sql
--
-- A counter per symbol, raised whenever candles_1m's history changes other
-- than at its head: backfill/history_backfill.py bumps it after inserting
-- or correcting candles (backfill/candle_store.py). The live indicator job
-- (streaming/indicator_state.py) compares it with the revision its state
-- was built from on every micro-batch, and rebuilds a symbol whose revision
-- moved from candles_1m: an EMA depends on every bar before it, so a
-- candle inserted in the past changes every value after it.
--
-- No row means revision 0: nothing has changed the history yet.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS candle_revisions (
    symbol         VARCHAR(20)  PRIMARY KEY,
    revision       BIGINT       NOT NULL,
    changed_at     TIMESTAMPTZ  NOT NULL,
    -- The oldest window_start the last change touched: for people reading
    -- the table; a rebuild always starts from the first stored candle.
    oldest_change  TIMESTAMPTZ
);
