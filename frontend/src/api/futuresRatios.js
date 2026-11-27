/**
 * futuresRatios.js
 *
 * The frontend's one client for backend/routers/futures_ratios.py's
 * GET /futures-ratios/{symbol}, and the parser for /ws/futures-ratios'
 * messages. Bars come back with times as Unix seconds (lightweight-charts'
 * unit) and every value as a number: { time, ratios: { kind: { ratio,
 * long, short } } }, a ratio missing from a bar it has no data in. Live
 * only: no pipeline.
 *
 * Author: @DS
 */

import { BACKEND_HTTP_URL } from "../config.js";

const toIso = (seconds) => new Date(seconds * 1000).toISOString();
const toSeconds = (iso) => Math.floor(new Date(iso).getTime() / 1000);

function parseRatios(ratios) {
  const parsed = {};
  for (const [kind, values] of Object.entries(ratios ?? {})) {
    if (!values || typeof values !== "object") continue;
    const ratio = values.ratio === null ? null : Number(values.ratio);
    parsed[kind] = { ratio: Number.isFinite(ratio) ? ratio : null, long: Number(values.long), short: Number(values.short) };
  }
  return parsed;
}

/** `symbol`'s ratios per `timeframe` bar starting in [start, end) (Unix seconds), oldest first. */
export async function fetchFuturesRatios(symbol, { timeframe, start, end }) {
  const params = new URLSearchParams({ timeframe, start: toIso(start), end: toIso(end) });
  const response = await fetch(`${BACKEND_HTTP_URL}/futures-ratios/${encodeURIComponent(symbol)}?${params}`);
  if (!response.ok) throw new Error(`GET /futures-ratios failed: ${response.status} ${response.statusText}`);
  const bars = await response.json();
  return bars.map((bar) => ({ time: toSeconds(bar.window_start), ratios: parseRatios(bar.ratios) }));
}

/** A /ws/futures-ratios message as { time, ratios }, or null (logged) if it isn't one. */
export function parseFuturesRatioMessage(data) {
  let message;
  try {
    message = JSON.parse(data);
  } catch {
    message = null;
  }
  if (!message || typeof message.window_start !== "string" || !message.ratios || typeof message.ratios !== "object") {
    console.warn("Ignoring a ratio message that isn't one:", data);
    return null;
  }
  const time = toSeconds(message.window_start);
  if (!Number.isFinite(time)) return null;
  return { time, ratios: parseRatios(message.ratios) };
}
