/**
 * useWhaleTrades.js
 *
 * Subscribes to backend/routers/whales.py's /ws/whales and exposes the
 * whale trades it pushes — every symbol's, newest first — plus the
 * connection's status for the right panel to show.
 *
 * On connect the backend first replays the latest BACKLOG alerts, then
 * pushes each new one; so a fresh page isn't empty, and a reconnect fills
 * in whatever was missed while disconnected. That replay repeats trades
 * already shown, and delivery is best-effort anyway (an alert can arrive
 * twice), so trades are deduplicated on (exchange, trade_id).
 *
 * A message that isn't a JSON object is logged and ignored. It used to be
 * parsed inside the state update, where the error unmounted the whole app
 * to a blank page — and since every connect replays the backlog, a reload
 * brought the blank page straight back until 50 newer whales pushed the
 * bad message out.
 *
 * Reconnects with backoff on any close it didn't ask for (a backend
 * restart, or a 1011 because the backend couldn't reach Kafka): see
 * lib/reconnectingSocket.js, shared with useLiveCandles.js.
 *
 * Each trade is the detector's alert JSON as-is: see
 * streaming/jobs/whale_detector.py's docstring for its fields.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { BACKEND_WS_URL } from "../config.js";
import { LIVE_FEED, withPipeline } from "../lib/pipelines.js";
import { openReconnectingSocket } from "../lib/reconnectingSocket.js";

// Alerts replayed on each connect — and the most trades kept, since the
// panel has no use for more than a fresh connection would show.
export const BACKLOG = 50;

/** "connecting" until the first open, "live" while open, "reconnecting" between attempts. */
export function useWhaleTrades(feed = LIVE_FEED) {
  const [trades, setTrades] = useState({ key: null, list: [] });
  const [status, setStatus] = useState("connecting");

  useEffect(() => {
    // A switch to or from a replay starts the list afresh: the backlog fills it.
    setTrades({ key: feed.key, list: [] });
    setStatus("connecting");
    const params = withPipeline(new URLSearchParams({ backlog: String(BACKLOG) }), feed.pipeline);
    return openReconnectingSocket(`${BACKEND_WS_URL}/ws/whales?${params}`, {
      onMessage: (data) => {
        const trade = parseTrade(data);
        if (trade) setTrades((previous) => ({ key: feed.key, list: addTrade(previous.key === feed.key ? previous.list : [], trade) }));
      },
      onStatus: setStatus,
    });
  }, [feed.key]);

  return { trades: trades.key === feed.key ? trades.list : [], status };
}

/** One message from /ws/whales as a trade, or null (logged) if it isn't a JSON object. */
export function parseTrade(data) {
  try {
    const trade = JSON.parse(data);
    if (trade !== null && typeof trade === "object" && !Array.isArray(trade)) return trade;
  } catch {
    // not JSON at all: reported below
  }
  console.error("Ignoring a whale alert that isn't a JSON object:", data);
  return null;
}

function tradeKey(trade) {
  return `${trade.exchange}:${trade.trade_id}`;
}

/**
 * `trades` with `trade` added at the front, unless it's already there.
 *
 * Front, because alerts arrive oldest first — the backlog in order, then
 * live — so each new one is the newest. A reconnect's replay only adds the
 * trades missed meanwhile, which are newer than everything kept.
 */
export function addTrade(trades, trade) {
  const key = tradeKey(trade);
  if (trades.some((existing) => tradeKey(existing) === key)) return trades;
  return [trade, ...trades].slice(0, BACKLOG);
}
