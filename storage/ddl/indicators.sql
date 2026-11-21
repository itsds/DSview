-- indicators.sql
--
-- Serving copy of the indicators streaming/jobs/indicators.py computes in
-- Spark: one row per symbol, timeframe and bar, with the EMA 20, 50 and
-- 200 (config/settings.py's SERVER_EMA_LENGTHS; one column each, NULL
-- while that EMA is still seeding). backend/postgres_client.py joins it
-- onto the candles it rolls up from candles_1m, by the same epoch-aligned
-- bar start, so GET /candles returns each candle with its indicators.
--
-- Upserted on every batch, like candles_1m (see postgres_sink.py): a bar's
-- row is rewritten while the bar is still provisional and converges on its
-- final values.
--
-- DOUBLE PRECISION, not NUMERIC: the values are the doubles the browser's
-- math.js would compute, and must reach it bit for bit.
--
-- Author: @DS

CREATE TABLE IF NOT EXISTS candle_indicators (
    symbol        VARCHAR(20)       NOT NULL,
    timeframe     VARCHAR(4)        NOT NULL,
    window_start  TIMESTAMPTZ       NOT NULL,
    ema_20        DOUBLE PRECISION,
    ema_50        DOUBLE PRECISION,
    ema_200       DOUBLE PRECISION,
    PRIMARY KEY (symbol, timeframe, window_start)
);
