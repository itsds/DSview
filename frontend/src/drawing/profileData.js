/**
 * profileData.js
 *
 * Volume profiles for the drawing tools (the fixed-range and anchored
 * volume profiles in tools.js), the way barData.js gives tools candles:
 * a synchronous get() a tool's draw() can call on every frame. The first
 * call for a range starts the fetch (GET /footprint/{symbol}/profile,
 * api/footprint.js) and returns null; once it lands, `onLoad` asks the
 * chart to redraw, and get() returns it from then on.
 *
 * A range that runs up to now (`end` null) is still filling, so it's
 * fetched again when it's older than LIVE_REFRESH_MS and drawn again —
 * which happens with every live candle — showing the previous profile in
 * the meantime. A closed range is fetched once. A failed fetch is retried
 * after RETRY_AFTER_MS.
 *
 * Built on footprint_1m, so a profile is real price-level volume, not a
 * guess spread over each bar's high and low: where the footprint has no
 * history (before the backfill's 30 days, by default), the profile says so.
 *
 * Author: @DS
 */

import { fetchProfile } from "../api/footprint.js";

export const LIVE_REFRESH_MS = 5_000;
const RETRY_AFTER_MS = 10_000;
const MAX_ENTRIES = 50; // profiles kept: drawings on screen, plus some dragged-through ranges

export function createProfileData({ symbol, pipeline, onLoad, fetch = fetchProfile, now = Date.now }) {
  const cache = new Map(); // key -> { profile, fetchedAt, loading, failedAt }

  function load(entry, request) {
    entry.loading = true;
    fetch(symbol, pipeline === undefined ? request : { ...request, pipeline })
      .then((profile) => {
        entry.profile = profile;
        entry.fetchedAt = now();
        entry.failedAt = null;
      })
      .catch((error) => {
        console.error("Loading a volume profile failed:", error);
        entry.failedAt = now();
      })
      .finally(() => {
        entry.loading = false;
        onLoad?.();
      });
  }

  return {
    /**
     * The volume profile of [start, end) (Unix seconds; `end` null: up to
     * now) in about `rows` rows, as api/footprint.js's fetchProfile()
     * returns it — or null until it has loaded.
     */
    get(start, end, rows) {
      const key = `${start}|${end ?? "now"}|${rows}`;
      let entry = cache.get(key);
      if (!entry) {
        if (cache.size >= MAX_ENTRIES) cache.delete(cache.keys().next().value); // the oldest
        cache.set(key, (entry = { profile: null, fetchedAt: null, loading: false, failedAt: null }));
      }
      const failedRecently = entry.failedAt !== null && now() - entry.failedAt < RETRY_AFTER_MS;
      const stale = end === null && entry.fetchedAt !== null && now() - entry.fetchedAt > LIVE_REFRESH_MS;
      if (!entry.loading && !failedRecently && (entry.profile === null || stale)) {
        load(entry, end === null ? { start, rows } : { start, end, rows });
      }
      return entry.profile;
    },
  };
}
