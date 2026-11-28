/**
 * futuresRatios.js
 *
 * Binance Futures' four long/short ratios (Phase 4c), as one chart holds
 * them: per bar of the chart's timeframe, the ratios GET /futures-ratios
 * rolled up from the perpetual's 5-minute periods (history) and
 * /ws/futures-ratios pushed (the bar in progress, and any bar a catch-up
 * changed). definitions.js's four ratio indicators read them through
 * ratioInput(); nothing else is computed here.
 *
 * Timeframes below the 5-minute period (1m) ask for 5m, and each bar shows
 * the period it falls in: there is nothing finer to show. A bar whose
 * period hasn't closed yet has no value until it does.
 *
 * The store fetches what the chart has loaded as it grows (ensure(), on
 * the first page and every scroll-back page), at most MAX_BARS_PER_REQUEST
 * bars a request, one request at a time; live bars win over history, as in
 * server.js. Live only: a replayed chart shows live's ratios for its bars.
 *
 * Author: @DS
 */

import { fetchFuturesRatios } from "../api/futuresRatios.js";
import { bucketStart, timeframeSeconds } from "../lib/timeframes.js";

// Must match config/settings.py's FUTURES_RATIO_KINDS and FUTURES_RATIO_PERIOD.
export const FUTURES_RATIO_KINDS = ["global_accounts", "top_accounts", "top_positions", "taker_volume"];
export const FUTURES_RATIO_PERIOD_SECONDS = 5 * 60;

// backend/routers/futures_ratios.py's MAX_BARS.
export const MAX_BARS_PER_REQUEST = 1000;

/** The timeframe to ask the backend for on a chart of `timeframe`: itself, or 5m on anything finer. */
export function ratioTimeframe(timeframe) {
  return timeframeSeconds(timeframe) < FUTURES_RATIO_PERIOD_SECONDS ? "5m" : timeframe;
}

/**
 * One chart's ratios. `fetch` is api/futuresRatios.js's (injectable for the
 * tests); `onChange` is called after every fetched page lands.
 */
export function createFuturesRatioStore({ symbol, timeframe, fetch = fetchFuturesRatios, onChange = () => {} }) {
  const askFor = ratioTimeframe(timeframe);
  const barSeconds = timeframeSeconds(askFor);
  const history = new Map();
  const live = new Map();
  let loadedFrom = null; // [loadedFrom, loadedTo) is fetched or being fetched
  let loadedTo = null;
  let queue = Promise.resolve();
  let destroyed = false;

  function enqueue(start, end) {
    for (let from = start; from < end; from += MAX_BARS_PER_REQUEST * barSeconds) {
      const to = Math.min(end, from + MAX_BARS_PER_REQUEST * barSeconds);
      queue = queue
        .then(async () => {
          if (destroyed) return;
          const bars = await fetch(symbol, { timeframe: askFor, start: from, end: to });
          if (destroyed) return;
          for (const bar of bars) history.set(bar.time, bar.ratios);
          onChange();
        })
        .catch((error) => console.error("Loading the long/short ratios failed:", error));
    }
  }

  return {
    timeframe: askFor,
    /** Make sure the bars from `from` to `to` (Unix seconds, `to` exclusive) are fetched or on their way. */
    ensure(from, to) {
      const start = bucketStart(from, barSeconds);
      const end = bucketStart(to - 1, barSeconds) + barSeconds;
      if (loadedFrom === null) {
        loadedFrom = start;
        loadedTo = end;
        enqueue(start, end);
        return;
      }
      if (start < loadedFrom) {
        enqueue(start, loadedFrom);
        loadedFrom = start;
      }
      if (end > loadedTo) {
        enqueue(loadedTo, end);
        loadedTo = end;
      }
    },
    /** One parseFuturesRatioMessage() result. */
    addLive({ time, ratios }) {
      live.set(time, ratios);
    },
    /** The ratios for the chart bar starting at `time`: its own, or on 1m, its 5-minute period's. */
    get(time) {
      const key = bucketStart(time, barSeconds);
      return live.get(key) ?? history.get(key);
    },
    /** Waits for every fetch asked for so far (the tests). */
    settled() {
      return queue;
    },
    destroy() {
      destroyed = true;
    },
  };
}

/**
 * What compute() gets as `ratios` for the chart's `bars`: series(kind, field)
 * is that ratio's `field` ("ratio", "long" or "short") for each bar, null
 * where there's none.
 */
export function ratioInput(store, bars) {
  return {
    series(kind, field = "ratio") {
      return bars.map((bar) => store?.get(bar.time)?.[kind]?.[field] ?? null);
    },
  };
}
