/**
 * keyboard.js
 *
 * Keyboard helpers shared by the chart's two sets of shortcuts: the
 * drawing controller's (Esc, Delete) and the drawing toolbar's tool keys.
 *
 * Author: @DS
 */

// Keys typed into a field, or pressed inside a dialog, belong to it, not the chart.
const OWNS_ITS_KEYS = "input, textarea, select, dialog, [contenteditable='true']";

/**
 * Whether a keydown is the chart's to act on: not typed into a field or
 * pressed in a dialog, and not already handled by something else on the
 * page — an open menu, say, marks the keys it uses with preventDefault().
 */
export function isChartKey(event) {
  return !event.defaultPrevented && !event.target.closest?.(OWNS_ITS_KEYS);
}

/**
 * Tool shortcuts are Alt plus a letter, maybe with Shift: { key: "T" } or
 * { key: "R", shift: true }. Matched on event.code, the physical key,
 * because on a Mac Option+T types "†" and that is what event.key holds.
 */
export function matchesShortcut(event, { key, shift = false }) {
  return event.altKey && !event.ctrlKey && !event.metaKey && event.shiftKey === shift && event.code === `Key${key}`;
}

/** "Alt + T", "Alt + Shift + R". */
export function formatShortcut({ key, shift = false }) {
  return ["Alt", shift && "Shift", key].filter(Boolean).join(" + ");
}
