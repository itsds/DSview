/**
 * useLiveCandles.js
 *
 * Subscribes to backend/routers/ws.py's /ws/candles/{symbol} endpoint and
 * exposes the latest candle as it streams in.
 *
 * Deliberately just the live tail — no history, no reconnect-with-backoff
 * yet. The backend has nothing to backfill from until the Postgres/
 * storage layer exists (see CLAUDE.md's Deployment approach: verify each
 * layer before adding the next).
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { BACKEND_WS_URL } from "../config.js";

export function useLiveCandles(symbol) {
  const [candle, setCandle] = useState(null);

  useEffect(() => {
    const ws = new WebSocket(`${BACKEND_WS_URL}/ws/candles/${symbol}`);

    ws.onmessage = (event) => {
      setCandle(JSON.parse(event.data));
    };

    return () => ws.close();
  }, [symbol]);

  return candle;
}
