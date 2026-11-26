/**
 * format.js
 *
 * How the results panel writes a backtest's numbers and times. Times in
 * UTC, like the chart's axis; a value that doesn't exist (no losing trade:
 * no profit factor) is a dash.
 *
 * Author: @DS
 */

const twoDecimals = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const whole = new Intl.NumberFormat("en-US");

const missing = (value) => value === null || value === undefined || Number.isNaN(value);

export function formatMoney(value) {
  if (missing(value)) return "—";
  return `${value < 0 ? "−" : ""}$${twoDecimals.format(Math.abs(value))}`;
}

export function formatSignedMoney(value) {
  if (missing(value)) return "—";
  return value > 0 ? `+${formatMoney(value)}` : formatMoney(value);
}

export function formatPct(value, { signed = false } = {}) {
  if (missing(value)) return "—";
  const sign = value < 0 ? "−" : signed && value > 0 ? "+" : "";
  return `${sign}${Math.abs(value).toFixed(2)}%`;
}

export function formatRatio(value) {
  return missing(value) ? "—" : value.toFixed(3);
}

export function formatPrice(value) {
  return missing(value) ? "—" : twoDecimals.format(value);
}

export function formatCount(value) {
  return missing(value) ? "—" : whole.format(value);
}

/** "2026-11-20 10:00" (UTC). */
export function formatTime(seconds) {
  return new Date(seconds * 1000).toISOString().slice(0, 16).replace("T", " ");
}

/** "2026-11-20" (UTC). */
export function formatDay(seconds) {
  return new Date(seconds * 1000).toISOString().slice(0, 10);
}

/** "ema_cross 20/50 · 1h": the strategy, its parameter values in order, the timeframe. */
export function runTitle(run) {
  const params = Object.values(run.params ?? {});
  return `${run.strategy}${params.length ? ` ${params.join("/")}` : ""} · ${run.timeframe}`;
}

/** The run picker's line: "#12 · ema_cross 20/50 · 1h · 2025-11-26 → 2026-11-26". */
export function runLabel(run) {
  return `#${run.id} · ${runTitle(run)} · ${formatDay(run.firstBar)} → ${formatDay(run.lastBar)}`;
}

/** What the check against Spark's EMAs found, in a few words; ok is true, false, or null (can't say). */
export function sparkCheckNote(check) {
  if (!check) return { text: "EMAs not checked against Spark", ok: null };
  if (check.differ > 0) return { text: `EMAs differ from Spark's on ${formatCount(check.differ)} bars`, ok: false };
  const equal = `EMAs equal Spark's on ${formatCount(check.equal)} of ${formatCount(check.bars)} bars`;
  if (check.missing > 0) return { text: `${equal}; ${formatCount(check.missing)} not computed by Spark yet`, ok: null };
  return { text: equal, ok: true };
}
