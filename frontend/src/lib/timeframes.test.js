import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { DAY_SECONDS, TIMEFRAMES, bucketStart, candleTime, rollUp, timeframeSeconds } from "./timeframes.js";

const T = Date.UTC(2026, 10, 20, 10, 0) / 1000; // 2026-11-20 10:00 UTC, Unix seconds

function minute(offset, { open, high, low, close, volume, trades = 1 }) {
  const start = new Date((T + offset * 60) * 1000).toISOString();
  // Strings, as the backend sends NUMERIC columns.
  return {
    symbol: "BTCUSDT",
    window_start: start,
    open: String(open),
    high: String(high),
    low: String(low),
    close: String(close),
    volume: String(volume),
    trade_count: trades,
  };
}

describe("timeframes", () => {
  it("lists the same timeframes, of the same lengths, as config/settings.py", () => {
    const settings = readFileSync(new URL("../../../config/settings.py", import.meta.url), "utf8");
    const start = settings.search(/^TIMEFRAMES\b/m);
    const block = settings.slice(start, settings.indexOf("}", start));
    const unitSeconds = { minutes: 60, hours: 3600, days: 86400 };
    const python = [...block.matchAll(/"([^"]+)": timedelta\((\w+)=(\d+)\)/g)].map(([, id, unit, n]) => ({
      id,
      seconds: unitSeconds[unit] * Number(n),
    }));
    expect(python.length).toBeGreaterThan(0);
    expect(TIMEFRAMES).toEqual(python);
  });

  it("knows each timeframe's length and rejects unknown ones", () => {
    expect(timeframeSeconds("5m")).toBe(300);
    expect(timeframeSeconds("1D")).toBe(DAY_SECONDS);
    expect(() => timeframeSeconds("2m")).toThrow("Unknown timeframe");
  });

  it("reads window_start with a Z or an offset", () => {
    expect(candleTime({ window_start: "2026-11-20T10:00:00Z" })).toBe(T);
    expect(candleTime({ window_start: "2026-11-20T15:30:00+05:30" })).toBe(T);
  });

  it("aligns buckets to the epoch, like the backend's date_bin()", () => {
    expect(bucketStart(T + 4 * 60 + 59, 300)).toBe(T);
    expect(bucketStart(T + 5 * 60, 300)).toBe(T + 300);
    expect(bucketStart(T + 3600 * 13, DAY_SECONDS)).toBe(T - 10 * 3600);
  });
});

describe("rollUp", () => {
  it("combines a bucket's minutes in time order, whatever order they arrive in", () => {
    const candle = rollUp(
      [
        minute(2, { open: 103, high: 108, low: 101, close: 104, volume: 1.5, trades: 3 }),
        minute(0, { open: 100, high: 102, low: 99, close: 101, volume: 2, trades: 5 }),
        minute(1, { open: 101, high: 105, low: 97, close: 103, volume: 0.5, trades: 1 }),
      ],
      T,
      300,
    );

    expect(candle).toEqual({
      symbol: "BTCUSDT",
      window_start: "2026-11-20T10:00:00.000Z",
      window_end: "2026-11-20T10:05:00.000Z",
      open: 100,
      high: 108,
      low: 97,
      close: 104,
      volume: 4,
      trade_count: 9,
    });
  });
});
