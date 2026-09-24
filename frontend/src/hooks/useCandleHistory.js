/**
 * useCandleHistory.js
 *
 * One-shot REST fetch of the newest page of candles for a symbol and
 * timeframe, used to seed the chart via setData() before live updates
 * take over via series.update() — see CandleChart.jsx, which also fetches
 * older pages itself as the user scrolls back.
 *
 * Returns an empty list — never the previous timeframe's candles — until
 * the current symbol/timeframe's page arrives, so the chart can't be
 * seeded with bars from the wrong timeframe.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { fetchCandles } from "../api/candles.js";

const NO_CANDLES = [];

export function useCandleHistory(symbol, timeframe) {
  const key = `${symbol}|${timeframe}`;
  const [loaded, setLoaded] = useState({ key: null, candles: NO_CANDLES });

  useEffect(() => {
    let cancelled = false;

    fetchCandles(symbol, { timeframe })
      .then((candles) => {
        if (!cancelled) setLoaded({ key, candles });
      })
      .catch((error) => console.error("Loading candle history failed:", error));

    return () => {
      cancelled = true;
    };
  }, [key, symbol, timeframe]);

  return loaded.key === key ? loaded.candles : NO_CANDLES;
}
