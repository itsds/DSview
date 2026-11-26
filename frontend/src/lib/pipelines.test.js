import { describe, expect, it } from "vitest";

import { LIVE, LIVE_FEED, REPLAY, feedFor, replayActive, replayFeed, withPipeline } from "./pipelines.js";

describe("withPipeline", () => {
  it("leaves live's URLs as they always were", () => {
    expect(withPipeline(new URLSearchParams({ timeframe: "1m" }), LIVE).toString()).toBe("timeframe=1m");
    expect(withPipeline(new URLSearchParams(), undefined).toString()).toBe("");
  });

  it("asks for the replay's data by name", () => {
    expect(withPipeline(new URLSearchParams({ timeframe: "5m" }), REPLAY).toString()).toBe("timeframe=5m&pipeline=replay");
  });
});

describe("feedFor", () => {
  const seeded = { state: "playing", symbol: "BTCUSDT", run: 3, seeded: true };

  it("shows a replay of this symbol once its history is in", () => {
    expect(feedFor(seeded, "BTCUSDT")).toEqual({ pipeline: REPLAY, key: "replay#3" });
    expect(feedFor({ ...seeded, state: "finished" }, "BTCUSDT").pipeline).toBe(REPLAY);
    expect(feedFor({ ...seeded, state: "starting" }, "BTCUSDT").pipeline).toBe(REPLAY);
  });

  it("stays on live until then, for another symbol, and when it failed or stopped", () => {
    for (const status of [
      null,
      { state: "unavailable" },
      { state: "idle" },
      { ...seeded, state: "starting", seeded: false },
      { ...seeded, symbol: "ETHUSDT" },
      { ...seeded, state: "failed" },
    ]) {
      expect(feedFor(status, "BTCUSDT")).toBe(LIVE_FEED);
    }
  });

  it("gives each run its own key, so a new run remounts the chart", () => {
    expect(replayFeed(1).key).not.toBe(replayFeed(2).key);
  });
});

describe("replayActive", () => {
  it("is true from starting to finished", () => {
    expect(["starting", "playing", "paused", "finished"].map((state) => replayActive({ state }))).toEqual([true, true, true, true]);
    expect(["idle", "failed", "unavailable"].map((state) => replayActive({ state }))).toEqual([false, false, false]);
    expect(replayActive(null)).toBe(false);
  });
});
