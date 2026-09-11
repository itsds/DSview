/**
 * useCandleHistory.js
 *
 * One-shot REST fetch of historical candles for a symbol, used to seed
 * the chart via setData() before useLiveCandles.js's WebSocket updates
 * take over via series.update() — see backend/routers/candles.py and
 * CandleChart.jsx.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

// TODO(@DS): move to a config module once the frontend has one; also
// needs to become an env-driven URL before this is ever deployed anywhere
// but localhost.
const BACKEND_HTTP_URL = "http://localhost:8000";

export function useCandleHistory(symbol) {
  const [history, setHistory] = useState([]);

  useEffect(() => {
    let cancelled = false;

    fetch(`${BACKEND_HTTP_URL}/candles/${symbol}`)
      .then((response) => response.json())
      .then((data) => {
        if (!cancelled) setHistory(data);
      });

    return () => {
      cancelled = true;
    };
  }, [symbol]);

  return history;
}
