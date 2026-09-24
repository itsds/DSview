/**
 * useTimeframeCandle.js
 *
 * Turns useLiveCandles.js's 1-minute stream into the live candle for the
 * chart's timeframe. 1m passes straight through; the backend only streams
 * 1m, so every bigger timeframe is rolled up here, in the same
 * epoch-aligned buckets the backend uses for history (lib/timeframes.js).
 *
 * The current bucket's 1m candles are kept in a Map keyed by start time
 * and re-aggregated on every update, rather than kept as running totals:
 * the backend re-sends the same 1m candle many times while it forms, so
 * adding each update's volume would count it again and again.
 *
 * The Map is seeded once over REST, so a bucket that was already in
 * progress at page load starts with its earlier minutes, not just the ones
 * that arrive live. Seeded minutes never overwrite live ones (live is
 * always newer), and nothing is published until the seed is in — a
 * live-only bar would briefly replace history's full bar with a partial one.
 *
 * Author: @DS
 */

import { useEffect, useRef, useState } from "react";

import { fetchCandles } from "../api/candles.js";
import { MINUTE_SECONDS, bucketStart, candleTime, rollUp, timeframeSeconds } from "../lib/timeframes.js";

function emptyBucket() {
  return { start: null, minutes: new Map(), seeded: false };
}

// Folds 1m candles into `bucket`, moving it forward whenever one starts a newer bucket.
function fold(bucket, minuteCandles, seconds, { overwrite }) {
  for (const candle of minuteCandles) {
    const time = candleTime(candle);
    const start = bucketStart(time, seconds);
    if (bucket.start === null || start > bucket.start) {
      bucket.start = start;
      bucket.minutes = new Map();
    }
    if (start < bucket.start) continue; // belongs to a bucket that's already finished
    if (overwrite || !bucket.minutes.has(time)) bucket.minutes.set(time, candle);
  }
}

export function useTimeframeCandle(symbol, timeframe, minuteCandle) {
  const seconds = timeframeSeconds(timeframe);
  const passThrough = seconds === MINUTE_SECONDS;
  const key = `${symbol}|${timeframe}`;
  const bucketRef = useRef(emptyBucket());
  const [rolled, setRolled] = useState({ key: null, candle: null });

  const publish = () => {
    const { start, minutes, seeded } = bucketRef.current;
    if (!seeded || start === null) return;
    setRolled({ key, candle: rollUp([...minutes.values()], start, seconds) });
  };

  // Reset and seed on every symbol/timeframe switch. Declared before the
  // live effect so that, on a switch, the reset runs before the current
  // minute gets folded in.
  useEffect(() => {
    bucketRef.current = emptyBucket();
    if (passThrough) return;

    let cancelled = false;
    fetchCandles(symbol, { timeframe: "1m", limit: seconds / MINUTE_SECONDS })
      .then((candles) => {
        if (cancelled) return;
        fold(bucketRef.current, candles, seconds, { overwrite: false });
        bucketRef.current.seeded = true;
        publish();
      })
      .catch((error) => {
        if (cancelled) return;
        // Degrade to rolling up only the minutes that arrive live.
        console.error("Seeding the live candle failed:", error);
        bucketRef.current.seeded = true;
        publish();
      });

    return () => {
      cancelled = true;
    };
  }, [key]);

  useEffect(() => {
    if (passThrough || !minuteCandle) return;
    fold(bucketRef.current, [minuteCandle], seconds, { overwrite: true });
    publish();
  }, [minuteCandle, key]);

  if (passThrough) return minuteCandle;
  return rolled.key === key ? rolled.candle : null;
}
