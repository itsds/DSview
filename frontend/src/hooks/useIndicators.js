/**
 * useIndicators.js
 *
 * The chart's active indicators — which ones, their parameters, colour slot
 * and visibility — persisted to localStorage so a reload keeps them.
 *
 * Each instance gets a colour slot when it's added and keeps it: colour
 * follows the indicator, so removing one never repaints the others.
 * Indicators over the candles share one pool of slots, since they overlap
 * each other; an indicator in its own pane always takes slot 0, as nothing
 * else draws there.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import { INDICATORS, MAX_PRICE_OVERLAYS, normalizeParams } from "../indicators/definitions.js";
import { newId } from "../lib/ids.js";

const STORAGE_KEY = "dsview.indicators";

function load() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "[]");
    if (!Array.isArray(saved)) return [];
    return saved
      .filter((item) => item && INDICATORS[item.type])
      .map((item) => ({
        id: String(item.id ?? newId()),
        type: item.type,
        params: normalizeParams(item.type, item.params),
        colorSlot: Number.isInteger(item.colorSlot) ? item.colorSlot : 0,
        visible: item.visible !== false,
      }));
  } catch {
    return []; // storage blocked, or a corrupt entry: start with none rather than crash
  }
}

// The lowest slot no other over-the-candles indicator holds, or null at the cap.
function freeColorSlot(indicators, type) {
  if (INDICATORS[type].pane !== "price") return 0;
  const taken = new Set(indicators.filter((i) => INDICATORS[i.type].pane === "price").map((i) => i.colorSlot));
  for (let slot = 0; slot < MAX_PRICE_OVERLAYS; slot += 1) {
    if (!taken.has(slot)) return slot;
  }
  return null;
}

export function useIndicators() {
  const [indicators, setIndicators] = useState(load);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(indicators));
    } catch {
      // Storage full or blocked: keep them for this session anyway.
    }
  }, [indicators]);

  const change = (id, apply) => setIndicators((list) => list.map((i) => (i.id === id ? apply(i) : i)));

  return {
    indicators,
    canAdd: (type) => freeColorSlot(indicators, type) !== null,
    add: (type) =>
      setIndicators((list) => {
        const colorSlot = freeColorSlot(list, type);
        if (colorSlot === null) return list;
        return [...list, { id: newId(), type, params: normalizeParams(type), colorSlot, visible: true }];
      }),
    setParams: (id, params) => change(id, (i) => ({ ...i, params: normalizeParams(i.type, params) })),
    toggle: (id) => change(id, (i) => ({ ...i, visible: !i.visible })),
    remove: (id) => setIndicators((list) => list.filter((i) => i.id !== id)),
  };
}
