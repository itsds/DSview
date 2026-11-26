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
import { LIVE_FEED, withPipeline } from "../lib/pipelines.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

export function useLiveCandles(symbol, feed = LIVE_FEED) {
  const key = `${symbol}|${feed.key}`;
  const [latest, setLatest] = useState({ key: null, candle: null });

  useEffect(() => {
    const params = withPipeline(new URLSearchParams(), feed.pipeline);
    const query = params.toString() ? `?${params}` : "";
    return openReconnectingSocket(`${BACKEND_WS_URL}/ws/candles/${symbol}${query}`, {
      onMessage: (data) => setLatest({ key, candle: JSON.parse(data) }),
    });
  }, [key]);

  // Never the last feed's candle: a switch to or from a replay starts with none.
  return latest.key === key ? latest.candle : null;
}
