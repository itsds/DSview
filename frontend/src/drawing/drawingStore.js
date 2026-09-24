/**
 * drawingStore.js
 *
 * The saved drawings for one symbol, persisted to localStorage so they
 * survive reloads and timeframe switches. Drawings are stored in
 * time/price terms, never pixels, so the same drawing lands correctly on
 * every timeframe (see coordinates.js).
 *
 * A tiny observable rather than React state: DrawingController.js
 * (outside React) writes to it on every placement, move, or delete, and
 * DrawingToolbar.jsx's "remove all" has to reach the same list.
 *
 * A drawing is { id, type, points: [{ time, price }, ...], text? } where
 * `time` is Unix seconds and `type` is a key of tools.js's TOOLS.
 *
 * Author: @DS
 */

const STORAGE_PREFIX = "dsview.drawings.";

function load(key) {
  try {
    const saved = JSON.parse(localStorage.getItem(key) ?? "[]");
    return Array.isArray(saved) ? saved : [];
  } catch {
    return []; // storage blocked, or a corrupt entry: start empty rather than crash
  }
}

export function createDrawingStore(symbol) {
  const key = STORAGE_PREFIX + symbol;
  const listeners = new Set();
  let drawings = load(key);

  const commit = (next) => {
    drawings = next;
    try {
      localStorage.setItem(key, JSON.stringify(drawings));
    } catch {
      // Storage full or blocked: keep the drawings for this session anyway.
    }
    listeners.forEach((listener) => listener(drawings));
  };

  return {
    getAll: () => drawings,
    add: (drawing) => commit([...drawings, drawing]),
    update: (id, patch) => commit(drawings.map((d) => (d.id === id ? { ...d, ...patch } : d))),
    remove: (id) => commit(drawings.filter((d) => d.id !== id)),
    clear: () => commit([]),
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}
