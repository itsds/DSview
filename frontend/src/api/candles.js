/**
 * candles.js
 *
 * The frontend's one client for backend/routers/candles.py's
 * GET /candles/{symbol} — shared by useCandleHistory.js (newest page),
 * CandleChart.jsx (older pages on scroll-back) and useTimeframeCandle.js
 * (seeding a live candle that's already in progress).
 *
 * Author: @DS
 */

// TODO(@DS): move to a config module once the frontend has one; also
// needs to become an env-driven URL before this is ever deployed anywhere
// but localhost.
const BACKEND_HTTP_URL = "http://localhost:8000";

// Candles per request. A page shorter than this means history has run out.
export const HISTORY_PAGE_SIZE = 300;

/**
 * Fetch candles for `symbol` at `timeframe`, oldest first. `before` (Unix
 * seconds) pages backwards: only candles starting before it are returned.
 */
export async function fetchCandles(symbol, { timeframe, limit = HISTORY_PAGE_SIZE, before } = {}) {
  const params = new URLSearchParams({ timeframe, limit: String(limit) });
  if (before !== undefined) params.set("before", new Date(before * 1000).toISOString());

  const response = await fetch(`${BACKEND_HTTP_URL}/candles/${encodeURIComponent(symbol)}?${params}`);
  if (!response.ok) {
    throw new Error(`GET /candles/${symbol} failed: ${response.status} ${response.statusText}`);
  }
  return response.json();
}
