/**
 * FootprintStore.js
 *
 * One chart's footprint data, plain JS (no chart, no network), so it's
 * tested without either. Two sources, as with candles:
 *
 *  - **History**: bars fetched over REST at a row size (GET /footprint),
 *    kept per row size: the rows of a $5 bar can't be split back into $1
 *    levels. Every bar of a fetched range is marked loaded, the ones
 *    without trades as empty, so they aren't asked for again.
 *  - **Live**: whole 1m grids at $1 levels from /ws/footprint, kept per
 *    bar and re-merged into any row size on demand. On 1m a minute is a
 *    bar, so its grid is the bar. On bigger timeframes the bar in progress
 *    also needs the minutes that closed before the page loaded:
 *    addLiveMinute() says when a bar still needs seeding with them (the
 *    caller fetches its minutes at $1 and hands them to seed()). Seeded
 *    minutes never overwrite live ones, which are always newer — the same
 *    rule as hooks/useTimeframeCandle.js. Until it's seeded, a bar shows
 *    its history, if any.
 *
 * Author: @DS
 */

import { MINUTE_SECONDS, bucketStart } from "../lib/timeframes.js";
import { combineLevels, mergeLevels } from "./levels.js";

export function createFootprintStore(barSeconds) {
  const history = new Map(); // row size -> Map<bar time, rows at that size>
  const live = new Map(); // bar time -> { minutes: Map<minute time, $1 levels>, seeded }

  function liveBar(time) {
    const entry = live.get(time);
    return entry?.seeded ? entry : null;
  }

  return {
    barSeconds,

    /** Bars fetched for [start, end) at `rowSize`, as [{ time, levels }]; every bar in the range counts as loaded. */
    addHistory(rowSize, start, end, bars) {
      let byTime = history.get(rowSize);
      if (!byTime) history.set(rowSize, (byTime = new Map()));
      for (let time = bucketStart(start, barSeconds); time < end; time += barSeconds) byTime.set(time, []);
      for (const bar of bars) byTime.set(bar.time, bar.levels);
    },

    /**
     * A minute's whole grid ($1 levels), live. Returns the start of its bar
     * if that bar still needs seeding with its earlier minutes, else null.
     */
    addLiveMinute(minuteTime, levels) {
      const bar = bucketStart(minuteTime, barSeconds);
      let entry = live.get(bar);
      if (!entry) live.set(bar, (entry = { minutes: new Map(), seeded: barSeconds === MINUTE_SECONDS }));
      entry.minutes.set(minuteTime, levels);
      return entry.seeded ? null : bar;
    },

    /** A bar's minutes from REST ([{ time, levels }] at $1), under the live ones. */
    seed(bar, minutes) {
      let entry = live.get(bar);
      if (!entry) live.set(bar, (entry = { minutes: new Map(), seeded: false }));
      for (const minute of minutes) {
        if (!entry.minutes.has(minute.time)) entry.minutes.set(minute.time, minute.levels);
      }
      entry.seeded = true;
    },

    /** The bar at `time` in rows of `rowSize`, prices ascending: [] for a bar without trades, null if not loaded. */
    bar(time, rowSize) {
      const entry = liveBar(time);
      if (entry) return mergeLevels(combineLevels([...entry.minutes.values()]), rowSize);
      return history.get(rowSize)?.get(time) ?? null;
    },

    /** Which of these bar times have nothing loaded at `rowSize`. */
    missing(times, rowSize) {
      const byTime = history.get(rowSize);
      return times.filter((time) => !liveBar(time) && !byTime?.has(time));
    },
  };
}
