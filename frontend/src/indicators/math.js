/**
 * math.js
 *
 * The indicator formulas, as pure functions over plain arrays. Every
 * function returns arrays the same length as its input, with null wherever
 * the indicator isn't defined yet (the first period − 1 bars of an SMA, for
 * instance), so outputs line up index-for-index with the bars they came
 * from.
 *
 * Author: @DS
 */

const DAY_SECONDS = 24 * 60 * 60;

export function sma(values, period) {
  const out = new Array(values.length).fill(null);
  let sum = 0;
  for (let i = 0; i < values.length; i += 1) {
    sum += values[i];
    if (i >= period) sum -= values[i - period];
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

// Exponential smoothing with factor `alpha`, seeded with the SMA of the first
// `period` defined values and null until then. Leading nulls in the input
// (MACD's line before its slow EMA exists) are skipped, not treated as 0.
function smooth(values, period, alpha) {
  const out = new Array(values.length).fill(null);
  let seedSum = 0;
  let seedCount = 0;
  let previous = null;
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i];
    if (value === null) continue;
    if (previous === null) {
      seedSum += value;
      seedCount += 1;
      if (seedCount === period) {
        previous = seedSum / period;
        out[i] = previous;
      }
      continue;
    }
    previous = alpha * value + (1 - alpha) * previous;
    out[i] = previous;
  }
  return out;
}

export function ema(values, period) {
  return smooth(values, period, 2 / (period + 1));
}

// Wilder's smoothing, which RSI is defined with.
function wilder(values, period) {
  return smooth(values, period, 1 / period);
}

export function rsi(closes, period) {
  const gains = closes.map((close, i) => (i === 0 ? null : Math.max(close - closes[i - 1], 0)));
  const losses = closes.map((close, i) => (i === 0 ? null : Math.max(closes[i - 1] - close, 0)));
  const averageGain = wilder(gains, period);
  const averageLoss = wilder(losses, period);
  return closes.map((_, i) => {
    if (averageGain[i] === null) return null;
    if (averageLoss[i] === 0) return averageGain[i] === 0 ? 50 : 100; // no losses: 100; no movement at all: neutral
    return 100 - 100 / (1 + averageGain[i] / averageLoss[i]);
  });
}

export function macd(closes, fastPeriod, slowPeriod, signalPeriod) {
  const fast = ema(closes, fastPeriod);
  const slow = ema(closes, slowPeriod);
  const line = closes.map((_, i) => (fast[i] === null || slow[i] === null ? null : fast[i] - slow[i]));
  const signal = ema(line, signalPeriod);
  const histogram = line.map((value, i) => (value === null || signal[i] === null ? null : value - signal[i]));
  return { line, signal, histogram };
}

export function bollinger(closes, period, multiplier) {
  const basis = sma(closes, period);
  const upper = new Array(closes.length).fill(null);
  const lower = new Array(closes.length).fill(null);
  for (let i = period - 1; i < closes.length; i += 1) {
    let squares = 0;
    for (let j = i - period + 1; j <= i; j += 1) squares += (closes[j] - basis[i]) ** 2;
    const deviation = Math.sqrt(squares / period); // population standard deviation
    upper[i] = basis[i] + multiplier * deviation;
    lower[i] = basis[i] - multiplier * deviation;
  }
  return { upper, basis, lower };
}

// Running VWAP of each bar's typical price, (high + low + close) / 3,
// starting over whenever sessionOf(bar) changes.
function runningVwap(bars, volumes, sessionOf) {
  const out = new Array(bars.length).fill(null);
  let session = null;
  let priceVolume = 0;
  let volume = 0;
  for (let i = 0; i < bars.length; i += 1) {
    const current = sessionOf(bars[i]);
    if (current !== session) {
      session = current;
      priceVolume = 0;
      volume = 0;
    }
    const typical = (bars[i].high + bars[i].low + bars[i].close) / 3;
    priceVolume += typical * volumes[i];
    volume += volumes[i];
    out[i] = volume === 0 ? typical : priceVolume / volume;
  }
  return out;
}

// Session VWAP, restarting at every UTC midnight — the usual session boundary
// for a market that trades around the clock.
export function vwap(bars, volumes) {
  return runningVwap(bars, volumes, (bar) => Math.floor(bar.time / DAY_SECONDS));
}

// VWAP that never restarts: every bar from the first one on. The Anchored
// VWAP drawing tool (drawing/tools.js) passes the bars from its anchor on.
export function anchoredVwap(bars, volumes) {
  return runningVwap(bars, volumes, () => 0);
}
