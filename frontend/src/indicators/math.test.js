import { describe, expect, it } from "vitest";

import { anchoredVwap, bollinger, ema, macd, rsi, sma, vwap } from "./math.js";

const DAY = 24 * 60 * 60;

describe("sma", () => {
  it("averages each window and is null until the first full one", () => {
    expect(sma([1, 2, 3, 4, 5], 3)).toEqual([null, null, 2, 3, 4]);
  });
});

describe("ema", () => {
  it("seeds with the SMA, then smooths with 2 / (period + 1)", () => {
    // Seed (1 + 2 + 3) / 3 = 2; then 0.5 * 4 + 0.5 * 2 = 3; then 0.5 * 5 + 0.5 * 3 = 4.
    expect(ema([1, 2, 3, 4, 5], 3)).toEqual([null, null, 2, 3, 4]);
    const out = ema([10, 10, 10, 20], 3);
    expect(out[3]).toBeCloseTo(15);
  });
});

describe("rsi", () => {
  it("is 100 with only gains and 50 with no movement", () => {
    expect(rsi([1, 2, 3, 4, 5, 6], 3).slice(3)).toEqual([100, 100, 100]);
    expect(rsi([5, 5, 5, 5, 5], 3).slice(3)).toEqual([50, 50]);
  });

  it("uses Wilder's smoothing", () => {
    // Changes: +2, -1, +2, -1. Seeds over 3: gain 4/3, loss 1/3 -> RS 4 -> RSI 80.
    // Next: gain (4/3 * 2 + 0) / 3 = 8/9, loss (1/3 * 2 + 1) / 3 = 5/9 -> RS 1.6 -> RSI 61.54.
    const out = rsi([10, 12, 11, 13, 12], 3);
    expect(out.slice(0, 3)).toEqual([null, null, null]);
    expect(out[3]).toBeCloseTo(80);
    expect(out[4]).toBeCloseTo(100 - 100 / 2.6);
  });
});

describe("macd", () => {
  it("lines up with its input and stays null until each part exists", () => {
    const closes = Array.from({ length: 12 }, (_, i) => 100 + i);
    const { line, signal, histogram } = macd(closes, 2, 4, 3);
    expect(line.length).toBe(12);
    expect(line.slice(0, 3)).toEqual([null, null, null]);
    expect(line[3]).not.toBeNull();
    expect(signal.slice(0, 5)).toEqual([null, null, null, null, null]); // 3 line values to seed
    expect(signal[5]).not.toBeNull();
    expect(histogram[5]).toBeCloseTo(line[5] - signal[5]);
  });
});

describe("bollinger", () => {
  it("collapses onto the basis when prices don't move", () => {
    const { upper, basis, lower } = bollinger([7, 7, 7, 7], 2, 2);
    expect(basis).toEqual([null, 7, 7, 7]);
    expect(upper).toEqual(basis);
    expect(lower).toEqual(basis);
  });

  it("uses the population standard deviation", () => {
    const { upper, lower } = bollinger([1, 3], 2, 1);
    expect(upper[1]).toBe(3); // basis 2, deviation 1
    expect(lower[1]).toBe(1);
  });
});

describe("vwap", () => {
  const bar = (time, price) => ({ time, high: price, low: price, close: price });

  it("restarts at every UTC midnight", () => {
    const bars = [bar(DAY - 120, 10), bar(DAY - 60, 20), bar(DAY, 30), bar(DAY + 60, 40)];
    expect(vwap(bars, [1, 1, 1, 3])).toEqual([10, 15, 30, 37.5]);
  });

  it("anchored never restarts", () => {
    const bars = [bar(DAY - 60, 20), bar(DAY, 30)];
    expect(anchoredVwap(bars, [1, 1])).toEqual([20, 25]);
  });

  it("falls back to the typical price with no volume", () => {
    expect(vwap([bar(0, 12)], [0])).toEqual([12]);
  });
});
