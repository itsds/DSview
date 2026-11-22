import { describe, expect, it } from "vitest";

import { createFootprintStore } from "./FootprintStore.js";

const T = Date.UTC(2026, 10, 20, 10, 0) / 1000; // 10:00, a 5m and 1m bar start
const level = (price, buy, sell) => ({ price, buy, sell });

describe("history", () => {
  it("keeps fetched bars per row size and marks bars without trades loaded", () => {
    const store = createFootprintStore(60);
    store.addHistory(5, T, T + 180, [{ time: T + 60, levels: [level(97000, 1, 2)] }]);

    expect(store.bar(T + 60, 5)).toEqual([level(97000, 1, 2)]);
    expect(store.bar(T, 5)).toEqual([]); // fetched, no trades
    expect(store.bar(T + 60, 10)).toBeNull(); // another row size: not loaded
    expect(store.missing([T, T + 60, T + 120, T + 180], 5)).toEqual([T + 180]);
  });

  it("replaces what an earlier fetch had", () => {
    const store = createFootprintStore(60);
    store.addHistory(1, T, T + 60, [{ time: T, levels: [level(1, 1, 0)] }]);
    store.addHistory(1, T, T + 60, []);
    expect(store.bar(T, 1)).toEqual([]);
  });
});

describe("live minutes", () => {
  it("are the bar on 1m, at any row size, and replace its history", () => {
    const store = createFootprintStore(60);
    store.addHistory(5, T, T + 60, [{ time: T, levels: [level(97000, 1, 0)] }]);

    expect(store.addLiveMinute(T, [level(97001, 1, 0), level(97004, 2, 1)])).toBeNull(); // nothing to seed
    store.addLiveMinute(T, [level(97001, 2, 0), level(97006, 0, 1)]); // the same minute, later: replaces
    expect(store.bar(T, 5)).toEqual([level(97000, 2, 0), level(97005, 0, 1)]);
    expect(store.bar(T, 1)).toEqual([level(97001, 2, 0), level(97006, 0, 1)]);
    expect(store.missing([T], 25)).toEqual([]);
  });

  it("on a bigger timeframe, wait for the bar's earlier minutes before replacing its history", () => {
    const store = createFootprintStore(300);
    store.addHistory(1, T, T + 300, [{ time: T, levels: [level(100, 5, 5)] }]);

    expect(store.addLiveMinute(T + 120, [level(100, 1, 0)])).toBe(T); // seed the 10:00 bar
    expect(store.bar(T, 1)).toEqual([level(100, 5, 5)]); // history until then

    store.addLiveMinute(T + 180, [level(101, 1, 0)]);
    store.seed(T, [
      { time: T, levels: [level(100, 2, 2)] },
      { time: T + 60, levels: [level(100, 1, 1)] },
      { time: T + 120, levels: [level(100, 9, 9)] }, // older than the live 10:02: ignored
    ]);
    expect(store.addLiveMinute(T + 240, [level(102, 0, 3)])).toBeNull(); // seeded now
    expect(store.bar(T, 1)).toEqual([level(100, 4, 3), level(101, 1, 0), level(102, 0, 3)]);
    expect(store.bar(T, 5)).toEqual([level(100, 5, 6)]);
  });
});
