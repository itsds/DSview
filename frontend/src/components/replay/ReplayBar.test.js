import { describe, expect, it } from "vitest";

import { speedLabel } from "../../api/replay.js";
import { replayStateLabel, replayTimeLabel } from "./ReplayBar.jsx";

describe("the replay bar's labels", () => {
  it("shows the replay's time in UTC, to the second", () => {
    expect(replayTimeLabel("2026-11-20T15:42:15.250+05:30")).toBe("2026-11-20 10:12:15 UTC");
    expect(replayTimeLabel(null)).toBe("");
  });

  it("says what the replay is doing", () => {
    expect(replayStateLabel({ state: "starting", phase: "starting the replay's Spark jobs" })).toBe(
      "Starting: starting the replay's Spark jobs…",
    );
    expect(replayStateLabel({ state: "paused" })).toBe("Paused");
    expect(replayStateLabel({ state: "failed", error: "no Bronze trades" })).toBe("Replay failed: no Bronze trades");
  });

  it("labels speeds as multiples, and Max", () => {
    expect([1, 10, 100, "max"].map(speedLabel)).toEqual(["1×", "10×", "100×", "Max"]);
  });
});
