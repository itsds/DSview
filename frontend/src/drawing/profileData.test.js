import { afterEach, describe, expect, it, vi } from "vitest";

import { LIVE_REFRESH_MS, createProfileData } from "./profileData.js";

const profile = (n) => ({ rowSize: 5, firstMinute: 0, lastMinute: 60, levels: [{ price: n, buy: 1, sell: 1 }] });

function setup() {
  let clock = 1_000_000;
  const requests = [];
  const pending = [];
  const fetch = vi.fn((symbol, request) => {
    requests.push({ symbol, ...request });
    return new Promise((resolve, reject) => pending.push({ resolve, reject }));
  });
  const onLoad = vi.fn();
  const profiles = createProfileData({ symbol: "BTCUSDT", onLoad, fetch, now: () => clock });
  return {
    profiles,
    requests,
    onLoad,
    settle: async (value, { fail = false } = {}) => {
      const next = pending.shift();
      if (fail) next.reject(new Error("down"));
      else next.resolve(value);
      await new Promise((r) => setTimeout(r, 0));
    },
    advance: (ms) => {
      clock += ms;
    },
  };
}

afterEach(() => vi.restoreAllMocks());

describe("createProfileData", () => {
  it("fetches a closed range once and redraws when it lands", async () => {
    const { profiles, requests, onLoad, settle } = setup();

    expect(profiles.get(100, 400, 24)).toBeNull();
    expect(profiles.get(100, 400, 24)).toBeNull(); // still loading: no second request
    expect(requests).toEqual([{ symbol: "BTCUSDT", start: 100, end: 400, rows: 24 }]);

    await settle(profile(1));
    expect(onLoad).toHaveBeenCalledTimes(1);
    expect(profiles.get(100, 400, 24)).toEqual(profile(1));
    expect(requests).toHaveLength(1);
  });

  it("refreshes a range that runs up to now, showing the old profile meanwhile", async () => {
    const { profiles, requests, settle, advance } = setup();
    profiles.get(100, null, 24);
    await settle(profile(1));
    expect(requests[0]).toEqual({ symbol: "BTCUSDT", start: 100, rows: 24 }); // no end: up to now

    advance(LIVE_REFRESH_MS - 1);
    expect(profiles.get(100, null, 24)).toEqual(profile(1));
    expect(requests).toHaveLength(1);

    advance(2);
    expect(profiles.get(100, null, 24)).toEqual(profile(1)); // stale: refetching, old one shown
    expect(requests).toHaveLength(2);
    await settle(profile(2));
    expect(profiles.get(100, null, 24)).toEqual(profile(2));
  });

  it("waits before retrying a failed fetch", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { profiles, requests, settle, advance } = setup();
    profiles.get(100, 400, 24);
    await settle(null, { fail: true });

    expect(profiles.get(100, 400, 24)).toBeNull();
    expect(requests).toHaveLength(1);
    advance(10_001);
    profiles.get(100, 400, 24);
    expect(requests).toHaveLength(2);
  });
});
