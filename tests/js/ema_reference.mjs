/**
 * ema_reference.mjs
 *
 * The browser's EMA, for the golden tests: rolls 1m candles up into each
 * timeframe's bars with frontend/src/lib/timeframes.js and runs
 * frontend/src/indicators/math.js's ema() over their closes — the exact
 * code the chart runs. tests/helpers.py's math_js_ema() calls it.
 *
 * stdin:  {"minutes": [[unix_seconds, "close"], ...], "timeframes": {"5m": 300, ...}, "lengths": [20, ...]}
 *         (final 1m candles, oldest first)
 * stdout: {"5m": {"starts": [...], "ema": {"20": [value | null, ...], ...}}, ...}
 *
 * Author: @DS
 */

import { readFileSync } from "node:fs";

import { ema } from "../../frontend/src/indicators/math.js";
import { bucketStart, rollUp } from "../../frontend/src/lib/timeframes.js";

const { minutes, timeframes, lengths } = JSON.parse(readFileSync(0, "utf8"));

const candles = minutes.map(([time, close]) => ({
  symbol: "GOLDEN",
  window_start: new Date(time * 1000).toISOString(),
  open: close,
  high: close,
  low: close,
  close,
  volume: "1",
  trade_count: 1,
}));

const out = {};
for (const [id, seconds] of Object.entries(timeframes)) {
  const buckets = new Map();
  minutes.forEach(([time], i) => {
    const start = bucketStart(time, seconds);
    if (!buckets.has(start)) buckets.set(start, []);
    buckets.get(start).push(candles[i]);
  });
  const starts = [...buckets.keys()].sort((a, b) => a - b);
  const closes = starts.map((start) => rollUp(buckets.get(start), start, seconds).close);
  out[id] = { starts, ema: Object.fromEntries(lengths.map((period) => [period, ema(closes, period)])) };
}
process.stdout.write(JSON.stringify(out));
