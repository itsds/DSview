import { readFileSync } from "node:fs";

import { afterEach, describe, expect, it, vi } from "vitest";

import { parseFuturesRatioMessage } from "../api/futuresRatios.js";
import { INDICATORS, usesFuturesRatios } from "./definitions.js";
import {
  FUTURES_RATIO_KINDS,
  FUTURES_RATIO_PERIOD_SECONDS,
  MAX_BARS_PER_REQUEST,
  createFuturesRatioStore,
  ratioInput,
  ratioTimeframe,
} from "./futuresRatios.js";

const T = Date.UTC(2026, 10, 20, 10, 0) / 1000; // a whole hour
const settings = readFileSync(new URL("../../../config/settings.py", import.meta.url), "utf8");
const ratios = (value) => ({ global_accounts: { ratio: value, long: 0.6, short: 0.4 } });

afterEach(() => vi.restoreAllMocks());

describe("the contract with config/settings.py", () => {
  it("lists the same ratios", () => {
    const [, kinds] = settings.match(/^FUTURES_RATIO_KINDS[^=]*= \(([^)]*)\)/m);
    expect(FUTURES_RATIO_KINDS).toEqual(kinds.split(",").map((kind) => kind.trim().replace(/"/g, "")));
  });

  it("stores the same period", () => {
    const [, minutes] = settings.match(/^FUTURES_RATIO_PERIOD[^=]*= timedelta\(minutes=(\d+)\)/m);
    expect(FUTURES_RATIO_PERIOD_SECONDS).toBe(Number(minutes) * 60);
  });

  it("has an indicator for every ratio", () => {
    const shown = Object.values(INDICATORS).map((def) => def.ratioKind).filter(Boolean);
    expect(shown.sort()).toEqual([...FUTURES_RATIO_KINDS].sort());
  });
});

describe("ratioTimeframe", () => {
  it("asks for 5m on anything finer, the chart's own otherwise", () => {
    expect(ratioTimeframe("1m")).toBe("5m");
    expect(ratioTimeframe("5m")).toBe("5m");
    expect(ratioTimeframe("1h")).toBe("1h");
    expect(ratioTimeframe("1D")).toBe("1D");
  });
});

describe("createFuturesRatioStore", () => {
  function storeWith(bars = []) {
    const fetch = vi.fn(async () => bars);
    const onChange = vi.fn();
    return { fetch, onChange, store: createFuturesRatioStore({ symbol: "BTCUSDT", timeframe: "1h", fetch, onChange }) };
  }

  it("fetches the loaded bars once, then only what scroll-back adds", async () => {
    const { fetch, onChange, store } = storeWith([{ time: T, ratios: ratios(1.5) }]);
    store.ensure(T, T + 3600 * 10);
    store.ensure(T + 1800, T + 3600 * 10); // inside what's loaded: nothing
    store.ensure(T - 3600 * 5, T + 3600 * 10); // five older bars
    await store.settled();

    expect(fetch.mock.calls.map(([, range]) => range)).toEqual([
      { timeframe: "1h", start: T, end: T + 3600 * 10 },
      { timeframe: "1h", start: T - 3600 * 5, end: T },
    ]);
    expect(onChange).toHaveBeenCalledTimes(2);
    expect(store.get(T)).toEqual(ratios(1.5));
  });

  it("splits a range into requests the backend takes", async () => {
    const fetch = vi.fn(async () => []);
    const store = createFuturesRatioStore({ symbol: "BTCUSDT", timeframe: "5m", fetch });
    store.ensure(T, T + 300 * (MAX_BARS_PER_REQUEST + 10));
    await store.settled();
    expect(fetch.mock.calls.map(([, { start, end }]) => (end - start) / 300)).toEqual([MAX_BARS_PER_REQUEST, 10]);
  });

  it("asks for 5m on a 1m chart and shows each period on its five minutes", async () => {
    const fetch = vi.fn(async () => [{ time: T, ratios: ratios(1.2) }]);
    const store = createFuturesRatioStore({ symbol: "BTCUSDT", timeframe: "1m", fetch });
    store.ensure(T + 60, T + 600);
    await store.settled();
    expect(fetch.mock.calls[0][1]).toEqual({ timeframe: "5m", start: T, end: T + 600 });
    for (const minute of [0, 1, 2, 3, 4]) expect(store.get(T + 60 * minute)).toEqual(ratios(1.2));
    expect(store.get(T + 300)).toBeUndefined();
  });

  it("lets a live bar win over the history fetched before it", async () => {
    const { store } = storeWith([{ time: T, ratios: ratios(1.5) }]);
    store.addLive({ time: T, ratios: ratios(1.7) });
    store.ensure(T, T + 3600);
    await store.settled();
    expect(store.get(T)).toEqual(ratios(1.7));
  });

  it("keeps going after a failed fetch, and drops pages landing after destroy()", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const fetch = vi.fn().mockRejectedValueOnce(new Error("down")).mockResolvedValue([{ time: T - 3600, ratios: ratios(2) }]);
    const onChange = vi.fn();
    const store = createFuturesRatioStore({ symbol: "BTCUSDT", timeframe: "1h", fetch, onChange });
    store.ensure(T, T + 3600);
    store.ensure(T - 3600, T + 3600);
    await store.settled();
    expect(store.get(T - 3600)).toEqual(ratios(2));

    store.destroy();
    store.ensure(T - 7200, T + 3600);
    await store.settled();
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

describe("the ratio indicators", () => {
  const bars = [{ time: T }, { time: T + 3600 }, { time: T + 7200 }];

  it("read each bar's ratio, null where there's none", () => {
    const store = { get: (time) => (time === T + 3600 ? ratios(1.92) : undefined) };
    const input = { bars, closes: [1, 2, 3], ratios: ratioInput(store, bars) };
    expect(INDICATORS.long_short_accounts.compute(input, {}).value).toEqual([null, 1.92, null]);
    expect(INDICATORS.taker_buy_sell.compute(input, {}).value).toEqual([null, null, null]);
    expect(ratioInput(store, bars).series("global_accounts", "long")).toEqual([null, 0.6, null]);
  });

  it("each go in their own pane, with a line at 1 and no settings", () => {
    for (const type of ["long_short_accounts", "top_long_short_accounts", "top_long_short_positions", "taker_buy_sell"]) {
      const def = INDICATORS[type];
      expect([def.pane, def.levels, def.params, def.source.includes("Binance Futures")]).toEqual(["own", [1], [], true]);
    }
  });

  it("are told apart from the computed ones", () => {
    expect(usesFuturesRatios([{ type: "ema" }, { type: "rsi" }])).toBe(false);
    expect(usesFuturesRatios([{ type: "ema" }, { type: "taker_buy_sell" }])).toBe(true);
    expect(usesFuturesRatios([{ type: "gone" }])).toBe(false);
  });
});

describe("parseFuturesRatioMessage", () => {
  it("reads a pushed bar, numbers as numbers", () => {
    const message = JSON.stringify({
      symbol: "BTCUSDT",
      timeframe: "1h",
      window_start: "2026-11-20T10:00:00+00:00",
      ratios: { taker_volume: { ratio: "1.12", long: "1520.5", short: "1357.1" }, top_accounts: { ratio: null, long: "0", short: "0" } },
    });
    expect(parseFuturesRatioMessage(message)).toEqual({
      time: T,
      ratios: { taker_volume: { ratio: 1.12, long: 1520.5, short: 1357.1 }, top_accounts: { ratio: null, long: 0, short: 0 } },
    });
  });

  it("ignores anything else", () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(parseFuturesRatioMessage("nope")).toBeNull();
    expect(parseFuturesRatioMessage(JSON.stringify({ window_start: "x" }))).toBeNull();
    expect(parseFuturesRatioMessage(JSON.stringify({ ratios: {} }))).toBeNull();
  });
});
