/**
 * footprint.js
 *
 * The frontend's one client for backend/routers/footprint.py: footprint
 * bars (GET /footprint/{symbol}), volume profiles
 * (GET /footprint/{symbol}/profile) and the parser for /ws/footprint's
 * messages. Everything comes back with times as Unix seconds
 * (lightweight-charts' unit) and levels as { price, buy, sell } numbers.
 *
 * Author: @DS
 */

import { BACKEND_HTTP_URL } from "../config.js";
import { parseLevels } from "../footprint/levels.js";

const toIso = (seconds) => new Date(seconds * 1000).toISOString();
const toSeconds = (iso) => Math.floor(new Date(iso).getTime() / 1000);

async function getJson(path, params) {
  const response = await fetch(`${BACKEND_HTTP_URL}${path}?${params}`);
  if (!response.ok) throw new Error(`GET ${path} failed: ${response.status} ${response.statusText}`);
  return response.json();
}

/**
 * The footprint of `symbol`'s `timeframe` bars starting in [start, end)
 * (Unix seconds), in rows `rowSize` wide: [{ time, levels }], oldest first.
 * Bars without trades aren't listed.
 */
export async function fetchFootprint(symbol, { timeframe, start, end, rowSize }) {
  const params = new URLSearchParams({ timeframe, start: toIso(start), end: toIso(end), row_size: String(rowSize) });
  const bars = await getJson(`/footprint/${encodeURIComponent(symbol)}`, params);
  return bars.map((bar) => ({ time: toSeconds(bar.window_start), levels: parseLevels(bar.levels) }));
}

/**
 * The volume profile of [start, end) (Unix seconds; `end` omitted: up to
 * now), in about `rows` rows or, given `rowSize`, rows that wide:
 * { rowSize, firstMinute, lastMinute, levels }, firstMinute/lastMinute
 * being the range's oldest and newest minute with any footprint (null
 * when it has none).
 */
export async function fetchProfile(symbol, { start, end, rows, rowSize }) {
  const params = new URLSearchParams({ start: toIso(start) });
  if (end !== undefined) params.set("end", toIso(end));
  if (rowSize !== undefined) params.set("row_size", String(rowSize));
  else if (rows !== undefined) params.set("rows", String(rows));
  const profile = await getJson(`/footprint/${encodeURIComponent(symbol)}/profile`, params);
  return {
    rowSize: profile.row_size === null ? null : Number(profile.row_size),
    firstMinute: profile.first_minute === null ? null : toSeconds(profile.first_minute),
    lastMinute: profile.last_minute === null ? null : toSeconds(profile.last_minute),
    levels: parseLevels(profile.levels),
  };
}

/** A /ws/footprint message as { time, levels } (one minute's whole grid), or null if it isn't one. */
export function parseFootprintMessage(data) {
  let message;
  try {
    message = JSON.parse(data);
  } catch {
    message = null;
  }
  if (!message || typeof message.window_start !== "string" || !Array.isArray(message.levels)) {
    console.warn("Ignoring a footprint message that isn't one:", data);
    return null;
  }
  return { time: toSeconds(message.window_start), levels: parseLevels(message.levels) };
}
