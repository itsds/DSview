/**
 * useLiveIndicators.js
 *
 * Subscribes to backend/routers/indicators.py's
 * /ws/indicators/{symbol}?timeframe=… and hands every update to
 * `onUpdate({ time, indicators })` as it arrives: Spark's values for the
 * bar in progress at this timeframe, and now and then a late revision of
 * the bar before it. On connect the backend first sends the newest bar's
 * cached values, so a reconnect resumes at once.
 *
 * A callback rather than React state: two messages can land before React
 * renders (a revision and the bar in progress, from one Spark batch), and
 * state would keep only the last. CandleChart.jsx files each one in its
 * server.js store and refreshes its indicators.
 *
 * Only connects while `enabled` — while the chart shows an indicator
 * Spark computes. History comes with the candles over REST either way.
 * Reconnects with backoff, like every backend socket
 * (lib/reconnectingSocket.js).
 *
 * Author: @DS
 */

import { useEffect, useRef } from "react";

import { BACKEND_WS_URL } from "../config.js";
import { parseIndicatorMessage } from "../indicators/server.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

export function useLiveIndicators(symbol, timeframe, enabled, onUpdate) {
  const callback = useRef(onUpdate);
  useEffect(() => {
    callback.current = onUpdate;
  });

  useEffect(() => {
    if (!enabled) return undefined;
    const params = new URLSearchParams({ timeframe });
    return openReconnectingSocket(`${BACKEND_WS_URL}/ws/indicators/${encodeURIComponent(symbol)}?${params}`, {
      onMessage: (data) => {
        const update = parseIndicatorMessage(data);
        if (update) callback.current(update);
      },
    });
  }, [symbol, timeframe, enabled]);
}
