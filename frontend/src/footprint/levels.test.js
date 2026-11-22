import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  FOOTPRINT_PRICE_STEP,
  FOOTPRINT_TIMEFRAMES,
  IMBALANCE_RATIO,
  MIN_ROW_PX,
  ROW_SIZES,
  barStats,
  combineLevels,
  formatVolume,
  mergeLevels,
  parseLevels,
  pickRowSize,
} from "./levels.js";

const settings = readFileSync(new URL("../../../config/settings.py", import.meta.url), "utf8");

describe("the footprint's contract", () => {
  it("matches config/settings.py's step and timeframes", () => {
    const [, step] = settings.match(/^FOOTPRINT_PRICE_STEP[^=]*= Decimal\("([^"]+)"\)/m);
    const [, timeframes] = settings.match(/^FOOTPRINT_TIMEFRAMES[^=]*= \(([^)]*)\)/m);
    expect(FOOTPRINT_PRICE_STEP).toBe(Number(step));
    expect(FOOTPRINT_TIMEFRAMES).toEqual([...timeframes.matchAll(/"([^"]+)"/g)].map((m) => m[1]));
  });

  it("only offers row sizes that are multiples of the step", () => {
    for (const size of ROW_SIZES) expect((size / FOOTPRINT_PRICE_STEP) % 1).toBe(0);
  });
});

const level = (price, buy, sell) => ({ price, buy, sell });

describe("parseLevels", () => {
  it("reads the backend's decimal strings as numbers", () => {
    expect(parseLevels([["97000", "1.25", "0"]])).toEqual([level(97000, 1.25, 0)]);
  });
});

describe("mergeLevels", () => {
  it("adds levels into rows named by their lower bound, prices ascending", () => {
    const merged = mergeLevels([level(97012, 1, 0), level(97003, 0.5, 2), level(97009, 1, 1), level(97000, 0, 1)], 5);
    expect(merged).toEqual([level(97000, 0.5, 3), level(97005, 1, 1), level(97010, 1, 0)]);
  });

  it("keeps $1 levels as they are", () => {
    expect(mergeLevels([level(97001, 1, 2), level(97000, 3, 4)], 1)).toEqual([level(97000, 3, 4), level(97001, 1, 2)]);
  });
});

describe("combineLevels", () => {
  it("sums several minutes level by level", () => {
    expect(combineLevels([[level(100, 1, 0), level(101, 0, 1)], [level(101, 2, 2)]])).toEqual([
      level(100, 1, 0),
      level(101, 2, 3),
    ]);
  });
});

describe("pickRowSize", () => {
  it("takes the smallest size whose rows are tall enough", () => {
    expect(pickRowSize(MIN_ROW_PX)).toBe(1); // 14 px a dollar: $1 rows
    expect(pickRowSize(3)).toBe(5); // $5 rows are 15 px
    expect(pickRowSize(MIN_ROW_PX / 10)).toBe(10);
    expect(pickRowSize(0.001)).toBeNull(); // even $1000 rows are 1 px
    expect(pickRowSize(null)).toBeNull();
  });

  it("uses a fixed size from the settings as it is", () => {
    expect(pickRowSize(0.001, 25)).toBe(25);
  });
});

describe("barStats", () => {
  it("finds the point of control, the delta and the biggest cell", () => {
    const stats = barStats([level(100, 1, 2), level(101, 4, 3), level(102, 0.5, 0)], 1);
    expect(stats.poc).toBe(101);
    expect(stats.delta).toBeCloseTo(5.5 - 5);
    expect(stats.total).toBeCloseTo(10.5);
    expect(stats.maxCell).toBe(4);
  });

  it("compares buys with the sells one row below, and sells with the buys one row above", () => {
    const rows = [level(100, 1, 1), level(101, IMBALANCE_RATIO, 9), level(102, 1, 2)];
    const stats = barStats(rows, 1);
    expect([...stats.buyImbalance]).toEqual([101]); // 3 bought at 101 against 1 sold at 100
    expect([...stats.sellImbalance]).toEqual([101]); // 9 sold at 101 against 1 bought at 102
  });

  it("doesn't compare across a gap or against nothing", () => {
    const stats = barStats([level(100, 0, 5), level(105, 50, 0), level(106, 10, 0)], 1);
    expect(stats.buyImbalance.size).toBe(0); // 105 is no neighbour of 100; 106 is, but 105 sold nothing
    expect(barStats([level(100, 50, 0), level(110, 0, 50)], 10).sellImbalance.size).toBe(0);
  });
});

describe("formatVolume", () => {
  it("fits a cell", () => {
    expect([0, 0.004, 0.42, 3.1, 3.175, 12.54, 128.4, 1234, 25000].map(formatVolume)).toEqual([
      "0", "<.01", "0.42", "3.1", "3.17", "12.5", "128", "1.2K", "25K",
    ]);
    expect(formatVolume(-1.5)).toBe("-1.5");
  });
});
