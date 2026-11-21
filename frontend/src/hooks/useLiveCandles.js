/**
 * useLiveCandles.js
 *
 * Subscribes to backend/routers/ws.py's /ws/candles/{symbol} endpoint and
 * exposes the latest 1-minute candle as it streams in.
 *
 * Reconnects with backoff (lib/reconnectingSocket.js), so the chart picks
 * up again by itself after a backend restart. On every connect the backend
 * first sends the current cached candle, so a reconnect resumes with the
 * minute in progress. Minutes that closed entirely while disconnected
 * aren't filled in until the page reloads — invisible after a reload's
 * second or two of downtime, a missing bar after a longer outage.
 *
 * Just the live tail: history is useCandleHistory.js's job.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { BACKEND_WS_URL } from "../config.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

export function useLiveCandles(symbol) {
  const [candle, setCandle] = useState(null);

  useEffect(
    () =>
      openReconnectingSocket(`${BACKEND_WS_URL}/ws/candles/${symbol}`, {
        onMessage: (data) => setCandle(JSON.parse(data)),
      }),
    [symbol],
  );

  return candle;
}
