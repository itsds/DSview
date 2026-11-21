import { describe, expect, it } from "vitest";

import { formatShortcut, isChartKey, matchesShortcut } from "./keyboard.js";

function keydown({ code = "KeyT", altKey = true, shiftKey = false, ctrlKey = false, metaKey = false, inside = null } = {}) {
  return {
    code,
    altKey,
    shiftKey,
    ctrlKey,
    metaKey,
    defaultPrevented: false,
    target: { closest: (selector) => (inside && selector.includes(inside) ? {} : null) },
  };
}

describe("keyboard", () => {
  it("matches Alt shortcuts on the physical key", () => {
    expect(matchesShortcut(keydown(), { key: "T" })).toBe(true);
    expect(matchesShortcut(keydown({ shiftKey: true, code: "KeyR" }), { key: "R", shift: true })).toBe(true);
    expect(matchesShortcut(keydown({ shiftKey: true }), { key: "T" })).toBe(false);
    expect(matchesShortcut(keydown({ ctrlKey: true }), { key: "T" })).toBe(false);
    expect(matchesShortcut(keydown({ altKey: false }), { key: "T" })).toBe(false);
  });

  it("formats shortcuts for tooltips", () => {
    expect(formatShortcut({ key: "T" })).toBe("Alt + T");
    expect(formatShortcut({ key: "R", shift: true })).toBe("Alt + Shift + R");
  });

  it("leaves keys typed into fields and dialogs, or already handled, alone", () => {
    expect(isChartKey(keydown())).toBe(true);
    expect(isChartKey(keydown({ inside: "input" }))).toBe(false);
    expect(isChartKey(keydown({ inside: "dialog" }))).toBe(false);
    expect(isChartKey({ ...keydown(), defaultPrevented: true })).toBe(false);
  });
});
