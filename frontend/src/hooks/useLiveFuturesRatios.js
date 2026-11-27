/**
 * useLiveFuturesRatios.js
 *
 * Subscribes to backend/routers/futures_ratios.py's
 * /ws/futures-ratios/{symbol}?timeframe=… and hands every bar to
 * `onUpdate({ time, ratios })` as it arrives: the newest bar on connect,
 * then each bar whose long/short ratios change (the bar in progress every
 * 5 minutes; after a catch-up, several older ones). `timeframe` is the
 * ratios' own (futuresRatios.js's ratioTimeframe(): 5m on a 1m chart).
 *
 * A callback rather than React state, as useLiveIndicators.js: a catch-up
 * sends several bars before React renders. Only connects while `enabled`
 * (while the chart shows a ratio indicator); reconnects with backoff
 * (lib/reconnectingSocket.js). Live only: no pipeline.
 *
 * Author: @DS
 */

import { useEffect, useRef } from "react";

import { parseFuturesRatioMessage } from "../api/futuresRatios.js";
import { BACKEND_WS_URL } from "../config.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

export function useLiveFuturesRatios(symbol, timeframe, enabled, onUpdate) {
  const callback = useRef(onUpdate);
  useEffect(() => {
    callback.current = onUpdate;
  });

  useEffect(() => {
    if (!enabled) return undefined;
    const params = new URLSearchParams({ timeframe });
    return openReconnectingSocket(`${BACKEND_WS_URL}/ws/futures-ratios/${encodeURIComponent(symbol)}?${params}`, {
      onMessage: (data) => {
        const update = parseFuturesRatioMessage(data);
        if (update) callback.current(update);
      },
    });
  }, [symbol, timeframe, enabled]);
}
