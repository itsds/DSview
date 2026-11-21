/**
 * timeframes.js
 *
 * Timeframe definitions and the bucket math shared by history and live
 * candles. Buckets are aligned to the Unix epoch — floor(t / size) * size
 * — which is exactly how backend/postgres_client.py's date_bin() query
 * rolls candles_1m up (and how streaming/indicator_engine.py buckets
 * indicators), so a live bar and a history bar for the same bucket
 * always get the same start time.
 *
 * Author: @DS
 */

export const MINUTE_SECONDS = 60;
export const DAY_SECONDS = 24 * 60 * 60;

// Must match config/settings.py's TIMEFRAMES.
export const TIMEFRAMES = [
  { id: "1m", seconds: MINUTE_SECONDS },
  { id: "5m", seconds: 5 * MINUTE_SECONDS },
  { id: "15m", seconds: 15 * MINUTE_SECONDS },
  { id: "1h", seconds: 60 * MINUTE_SECONDS },
  { id: "4h", seconds: 4 * 60 * MINUTE_SECONDS },
  { id: "1D", seconds: DAY_SECONDS },
];

export function timeframeSeconds(id) {
  const timeframe = TIMEFRAMES.find((tf) => tf.id === id);
  if (!timeframe) throw new Error(`Unknown timeframe: ${id}`);
  return timeframe.seconds;
}

/** A candle's window_start as Unix seconds — lightweight-charts' time unit. */
export function candleTime(candle) {
  return Math.floor(new Date(candle.window_start).getTime() / 1000);
}

export function bucketStart(time, seconds) {
  return Math.floor(time / seconds) * seconds;
}

/**
 * Combines the 1m candles of one bucket into a single candle, in the same
 * shape the backend sends, so CandleChart.jsx treats both alike.
 */
export function rollUp(minuteCandles, start, seconds) {
  const sorted = [...minuteCandles].sort((a, b) => candleTime(a) - candleTime(b));
  return {
    symbol: sorted[0].symbol,
    window_start: new Date(start * 1000).toISOString(),
    window_end: new Date((start + seconds) * 1000).toISOString(),
    open: Number(sorted[0].open),
    high: Math.max(...sorted.map((c) => Number(c.high))),
    low: Math.min(...sorted.map((c) => Number(c.low))),
    close: Number(sorted[sorted.length - 1].close),
    volume: sorted.reduce((sum, c) => sum + Number(c.volume), 0),
    trade_count: sorted.reduce((sum, c) => sum + Number(c.trade_count), 0),
  };
}
