import { readFileSync } from "node:fs";

import { afterEach, describe, expect, it, vi } from "vitest";

import { INDICATORS } from "./definitions.js";
import { ema } from "./math.js";
import {
  SERVER_EMA_LENGTHS,
  createServerIndicatorStore,
  parseIndicatorMessage,
  serverColumn,
  serverInput,
} from "./server.js";

const T = Date.UTC(2026, 10, 20, 10, 0) / 1000;
const iso = (time) => new Date(time * 1000).toISOString();
const candle = (time, indicators) => ({ window_start: iso(time), close: "1", indicators });
const bars = (count) => Array.from({ length: count }, (_, i) => ({ time: T + 60 * i, close: 100 + i }));

afterEach(() => vi.restoreAllMocks());

describe("SERVER_EMA_LENGTHS", () => {
  it("matches config/settings.py", () => {
    const settings = readFileSync(new URL("../../../config/settings.py", import.meta.url), "utf8");
    const [, lengths] = settings.match(/^SERVER_EMA_LENGTHS[^=]*= \(([^)]*)\)/m);
    expect(SERVER_EMA_LENGTHS).toEqual(lengths.split(",").map(Number));
  });
});

describe("serverColumn", () => {
  it("names the column for an EMA Spark computes, and nothing else", () => {
    expect(serverColumn("ema", { period: 20 })).toBe("ema_20");
    expect(serverColumn("ema", { period: 200 })).toBe("ema_200");
    expect(serverColumn("ema", { period: 21 })).toBeNull();
    expect(serverColumn("sma", { period: 20 })).toBeNull();
  });
});

describe("createServerIndicatorStore", () => {
  it("keeps every page's values and lets live values win", () => {
    const store = createServerIndicatorStore();
    store.addHistory([candle(T, { ema_20: 1 }), candle(T + 60, { ema_20: 2 }), { window_start: iso(T + 120) }]);
    store.addLive({ time: T + 60, indicators: { ema_20: 3 } });
    store.addHistory([candle(T + 60, { ema_20: 2.5 })]); // a page fetched before that live update arrived

    expect(store.get(T)).toEqual({ ema_20: 1 });
    expect(store.get(T + 60)).toEqual({ ema_20: 3 });
    expect(store.get(T + 120)).toBeUndefined();
  });
});

describe("serverInput", () => {
  it("lines a column up with the bars, null where a bar has no value", () => {
    const store = createServerIndicatorStore();
    store.addHistory([candle(T + 60, { ema_20: 5, ema_50: null })]);
    const server = serverInput(store, bars(3));

    expect(server.series("ema_20")).toEqual([null, 5, null]);
    expect([...server.used]).toEqual(["ema_20"]);
  });

  it("gives null — fall back to the browser — when the server has nothing for these bars", () => {
    const store = createServerIndicatorStore();
    store.addHistory([candle(T, { ema_20: null, ema_50: null, ema_200: null })]);
    const server = serverInput(store, bars(3));

    expect(server.series("ema_20")).toBeNull();
    expect(server.series(null)).toBeNull();
    expect(server.used.size).toBe(0);
  });
});

describe("the EMA's compute()", () => {
  const input = (store, count) => {
    const chartBars = bars(count);
    return { bars: chartBars, closes: chartBars.map((bar) => bar.close), server: serverInput(store, chartBars) };
  };

  it("uses Spark's values for its lengths when it has them", () => {
    const store = createServerIndicatorStore();
    store.addHistory([candle(T + 120, { ema_20: 42 })]);
    expect(INDICATORS.ema.compute(input(store, 3), { period: 20 }).value).toEqual([null, null, 42]);
  });

  it("computes in the browser otherwise", () => {
    const store = createServerIndicatorStore();
    store.addHistory([candle(T + 120, { ema_20: 42 })]);
    const closes = bars(25).map((bar) => bar.close);
    expect(INDICATORS.ema.compute(input(store, 25), { period: 21 }).value).toEqual(ema(closes, 21));
    expect(INDICATORS.ema.compute(input(createServerIndicatorStore(), 25), { period: 20 }).value).toEqual(
      ema(closes, 20),
    );
  });
});

describe("parseIndicatorMessage", () => {
  it("reads a /ws/indicators message", () => {
    const message = { symbol: "BTCUSDT", timeframe: "5m", window_start: "2026-11-20T10:00:00+00:00", indicators: { ema_20: 1.5 } };
    expect(parseIndicatorMessage(JSON.stringify(message))).toEqual({ time: T, indicators: { ema_20: 1.5 } });
  });

  it("ignores, and logs, anything else", () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(parseIndicatorMessage("not json")).toBeNull();
    expect(parseIndicatorMessage(JSON.stringify({ indicators: {} }))).toBeNull();
    expect(parseIndicatorMessage(JSON.stringify({ window_start: "soon", indicators: {} }))).toBeNull();
    expect(parseIndicatorMessage(JSON.stringify({ window_start: "2026-11-20T10:00:00Z", indicators: null }))).toBeNull();
    expect(error).toHaveBeenCalledTimes(4);
  });
});
