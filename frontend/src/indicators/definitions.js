/**
 * definitions.js
 *
 * Every indicator the chart offers: its name, parameters, where it draws
 * (over the candles, or in its own pane below them), the lines it's made
 * of, and how to compute them from the chart's bars.
 *
 * compute() is the single place values come from. It runs in the browser
 * for now; when roadmap item 2 moves EMA/RSI into Spark, their compute() is
 * what gets replaced by the backend's values — nothing that draws or lists
 * indicators has to change.
 *
 * Colour: each instance keeps the colour slot it was given when added (see
 * useIndicators.js). Slots are theme.css's three validated series hues, then
 * the same three dashed — so indicators over the candles cap at six rather
 * than get a seventh, indistinguishable colour.
 *
 * Author: @DS
 */

import { bollinger, ema, macd, rsi, sma, vwap } from "./math.js";

export const SERIES_HUES = 3; // theme.css --color-series-1..3
export const MAX_PRICE_OVERLAYS = SERIES_HUES * 2;

export function slotStyle(slot, theme) {
  return { color: theme.series[slot % SERIES_HUES], dashed: slot >= SERIES_HUES };
}

function lengthParam(defaultValue, min = 1) {
  return { key: "period", label: "Length", default: defaultValue, min, max: 500 };
}

// Line fields: `type` "line" or "histogram" (coloured by sign), `lineWidth`,
// `colorOffset` (a second line of the same indicator takes the next slot),
// and `axisLabel: false` to keep its last value off the price scale.
export const INDICATORS = {
  sma: {
    name: "Simple Moving Average",
    shortName: "SMA",
    description: "Average close over the last N bars.",
    pane: "price",
    params: [lengthParam(20)],
    lines: [{ key: "value", label: "SMA", type: "line" }],
    compute: ({ closes }, { period }) => ({ value: sma(closes, period) }),
  },
  ema: {
    name: "Exponential Moving Average",
    shortName: "EMA",
    description: "Moving average that weights recent bars more.",
    pane: "price",
    params: [lengthParam(20)],
    lines: [{ key: "value", label: "EMA", type: "line" }],
    compute: ({ closes }, { period }) => ({ value: ema(closes, period) }),
  },
  bb: {
    name: "Bollinger Bands",
    shortName: "BB",
    description: "Moving average with bands a set number of standard deviations above and below.",
    pane: "price",
    params: [lengthParam(20, 2), { key: "multiplier", label: "Std. dev.", default: 2, min: 0.1, max: 10, step: 0.1 }],
    lines: [
      { key: "upper", label: "Upper", type: "line", lineWidth: 1, axisLabel: false },
      { key: "basis", label: "Basis", type: "line", lineWidth: 1 },
      { key: "lower", label: "Lower", type: "line", lineWidth: 1, axisLabel: false },
    ],
    compute: ({ closes }, { period, multiplier }) => bollinger(closes, period, multiplier),
  },
  vwap: {
    name: "Volume Weighted Average Price",
    shortName: "VWAP",
    description: "Average price weighted by volume, restarting each day (UTC).",
    pane: "price",
    params: [],
    lines: [{ key: "value", label: "VWAP", type: "line" }],
    compute: ({ bars, volumes }) => ({ value: vwap(bars, volumes) }),
  },
  rsi: {
    name: "Relative Strength Index",
    shortName: "RSI",
    description: "Momentum from 0 to 100; above 70 is often read as overbought, below 30 as oversold.",
    pane: "own",
    range: { min: 0, max: 100 },
    levels: [30, 70],
    params: [lengthParam(14, 2)],
    lines: [{ key: "value", label: "RSI", type: "line" }],
    compute: ({ closes }, { period }) => ({ value: rsi(closes, period) }),
  },
  macd: {
    name: "Moving Average Convergence Divergence",
    shortName: "MACD",
    description: "Gap between a fast and a slow EMA, with a signal line and histogram.",
    pane: "own",
    baseline: 0,
    params: [
      { key: "fast", label: "Fast length", default: 12, min: 1, max: 500 },
      { key: "slow", label: "Slow length", default: 26, min: 1, max: 500 },
      { key: "signal", label: "Signal length", default: 9, min: 1, max: 500 },
    ],
    lines: [
      { key: "histogram", label: "Histogram", type: "histogram", axisLabel: false },
      { key: "line", label: "MACD", type: "line" },
      { key: "signal", label: "Signal", type: "line", colorOffset: 1 },
    ],
    compute: ({ closes }, { fast, slow, signal }) => macd(closes, fast, slow, signal),
  },
};

export function indicatorLabel({ type, params }) {
  const def = INDICATORS[type];
  return [def.shortName, ...def.params.map((param) => params[param.key])].join(" ");
}

/** `params` with defaults filled in and every value clamped to its allowed range. */
export function normalizeParams(type, params = {}) {
  return Object.fromEntries(
    INDICATORS[type].params.map((param) => {
      const raw = Number(params[param.key]);
      let value = Number.isFinite(raw) ? raw : param.default;
      if (!param.step) value = Math.round(value);
      return [param.key, Math.min(param.max, Math.max(param.min, value))];
    }),
  );
}
