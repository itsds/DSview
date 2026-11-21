/**
 * server.js
 *
 * The indicator values Spark computes (streaming/jobs/indicators.py), as
 * the chart receives them: EMA 20, 50 and 200 on every timeframe, from
 * each candle's `indicators` in GET /candles (history, scroll-back pages
 * included) and from /ws/indicators (the bar in progress, and now and then
 * a late revision of the one before).
 *
 * definitions.js's compute() is where they replace the browser's own: an
 * EMA at one of SERVER_EMA_LENGTHS uses the server's values whenever the
 * server has any for the loaded bars, and math.js's otherwise — for any
 * other length, and when the indicator job isn't (or wasn't yet) running.
 * Never a mix of the two in one line: the browser seeds its EMA from the
 * first loaded bar and Spark from all of history, so their values differ
 * near the left edge, and a line switching between them would jump.
 *
 * Author: @DS
 */

import { candleTime } from "../lib/timeframes.js";

// Must match config/settings.py's SERVER_EMA_LENGTHS (timeframes are all of
// lib/timeframes.js's TIMEFRAMES).
export const SERVER_EMA_LENGTHS = [20, 50, 200];

/** The server's column for an indicator with these params ("ema_20"), or null if Spark doesn't compute it. */
export function serverColumn(type, params) {
  if (type === "ema" && SERVER_EMA_LENGTHS.includes(params.period)) return `ema_${params.period}`;
  return null;
}

/**
 * Every server value one chart has received, by bar time. Live messages
 * win over history: a history page fetched a moment before a live message
 * arrived holds the older value (useTimeframeCandle.js seeds the same way).
 */
export function createServerIndicatorStore() {
  const history = new Map();
  const live = new Map();
  return {
    /** Candles from GET /candles, each with its `indicators`. */
    addHistory(candles) {
      for (const candle of candles) {
        if (candle.indicators) history.set(candleTime(candle), candle.indicators);
      }
    },
    /** One parseIndicatorMessage() result. */
    addLive({ time, indicators }) {
      live.set(time, indicators);
    },
    get(time) {
      return live.get(time) ?? history.get(time);
    },
  };
}

/**
 * What compute() gets as `server` for the chart's current `bars`:
 * series(column) is that column's value for each bar (null where there's
 * none), or null if the server has no value at all for these bars — the
 * cue to fall back to the browser's own. `used` collects the columns that
 * came from the server, so IndicatorManager notices a line switching source.
 */
export function serverInput(store, bars) {
  const used = new Set();
  return {
    used,
    series(column) {
      if (column === null) return null;
      const values = bars.map((bar) => store.get(bar.time)?.[column] ?? null);
      if (!values.some((value) => value !== null)) return null;
      used.add(column);
      return values;
    },
  };
}

/** A /ws/indicators message as { time, indicators }, or null (logged) if it isn't one. */
export function parseIndicatorMessage(data) {
  try {
    const message = JSON.parse(data);
    if (
      message !== null &&
      typeof message === "object" &&
      typeof message.window_start === "string" &&
      message.indicators !== null &&
      typeof message.indicators === "object"
    ) {
      const time = candleTime(message);
      if (Number.isFinite(time)) return { time, indicators: message.indicators };
    }
  } catch {
    // not JSON at all: reported below
  }
  console.error("Ignoring an indicator update that isn't one:", data);
  return null;
}
