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
 * Reconnects on any close the hook didn't ask for — the backend restarting
 * (uvicorn --reload does that on every save), or a 1011 because it couldn't
 * reach Kafka — with exponential backoff. A socket that was open for a
 * while before closing starts over at the shortest delay; one that opens
 * and is closed straight away (1011 again) keeps backing off, so a broker
 * that's down isn't hammered once a second.
 *
 * Each trade is the detector's alert JSON as-is: see
 * streaming/jobs/whale_detector.py's docstring for its fields.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { BACKEND_WS_URL } from "../config.js";

// Alerts replayed on each connect — and the most trades kept, since the
// panel has no use for more than a fresh connection would show.
const BACKLOG = 50;

const RETRY_INITIAL_MS = 1_000;
const RETRY_MAX_MS = 30_000;
// Open at least this long before closing = a real connection, not a refusal.
const STABLE_MS = 10_000;

/** "connecting" until the first open, "live" while open, "reconnecting" between attempts. */
export function useWhaleTrades() {
  const [trades, setTrades] = useState([]);
  const [status, setStatus] = useState("connecting");

  useEffect(() => {
    let ws = null;
    let retryTimer = null;
    let retryMs = RETRY_INITIAL_MS;
    let openedAt = 0;
    let stopped = false;

    function connect() {
      ws = new WebSocket(`${BACKEND_WS_URL}/ws/whales?backlog=${BACKLOG}`);

      ws.onopen = () => {
        openedAt = Date.now();
        setStatus("live");
      };

      ws.onmessage = (event) => {
        if (stopped) return;
        const trade = JSON.parse(event.data);
        setTrades((previous) => addTrade(previous, trade));
      };

      // Browsers follow every failed or dropped socket's error event with a
      // close, so reconnecting is handled here alone.
      ws.onclose = () => {
        if (stopped) return;
        if (openedAt && Date.now() - openedAt >= STABLE_MS) retryMs = RETRY_INITIAL_MS;
        openedAt = 0;
        setStatus("reconnecting");
        retryTimer = setTimeout(connect, retryMs);
        retryMs = Math.min(retryMs * 2, RETRY_MAX_MS);
      };
    }

    connect();

    return () => {
      stopped = true;
      clearTimeout(retryTimer);
      ws.close();
    };
  }, []);

  return { trades, status };
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
function addTrade(trades, trade) {
  const key = tradeKey(trade);
  if (trades.some((existing) => tradeKey(existing) === key)) return trades;
  return [trade, ...trades].slice(0, BACKLOG);
}
