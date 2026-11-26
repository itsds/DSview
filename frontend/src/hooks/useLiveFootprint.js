/**
 * useLiveFootprint.js
 *
 * Subscribes to backend/routers/footprint.py's /ws/footprint/{symbol} and
 * hands every message to `onUpdate({ time, levels })`: one minute's whole
 * grid at $1 levels, mostly the minute in progress, now and then a late
 * revision of the one before. On connect the backend first sends the
 * newest minute's cached grid, so a reconnect resumes at once.
 *
 * A callback rather than React state, as in useLiveIndicators.js: two
 * minutes can arrive in one Spark batch. CandleChart.jsx passes each to its
 * FootprintManager.
 *
 * Only connects while `enabled` — while the chart draws a footprint.
 * Reconnects with backoff, like every backend socket
 * (lib/reconnectingSocket.js).
 *
 * Author: @DS
 */

import { useEffect, useRef } from "react";

import { parseFootprintMessage } from "../api/footprint.js";
import { BACKEND_WS_URL } from "../config.js";
import { LIVE, withPipeline } from "../lib/pipelines.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

export function useLiveFootprint(symbol, enabled, onUpdate, pipeline = LIVE) {
  const callback = useRef(onUpdate);
  useEffect(() => {
    callback.current = onUpdate;
  });

  useEffect(() => {
    if (!enabled) return undefined;
    const params = withPipeline(new URLSearchParams(), pipeline);
    const query = params.toString() ? `?${params}` : "";
    return openReconnectingSocket(`${BACKEND_WS_URL}/ws/footprint/${encodeURIComponent(symbol)}${query}`, {
      onMessage: (data) => {
        const update = parseFootprintMessage(data);
        if (update) callback.current(update);
      },
    });
  }, [symbol, enabled, pipeline]);
}
