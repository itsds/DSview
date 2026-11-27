import { describe, expect, it } from "vitest";

import { formatReturn, markersWithin, tradeMarkers } from "./markers.js";

const HOUR = 3600;
const T0 = 1_780_000_000 - (1_780_000_000 % (4 * HOUR)); // a 4h bar's start
const COLORS = { long: "green", short: "red", profit: "up", loss: "down" };

const trade = (number, side, entry, exit, pnl, extra = {}) => ({
  number,
  side,
  entryTime: T0 + entry * HOUR,
  exitTime: T0 + exit * HOUR,
  pnl,
  returnPct: pnl / 100,
  closedByEnd: false,
  ...extra,
});

describe("tradeMarkers", () => {
  it("marks a long's entry under the bar and its exit over it, with the result", () => {
    const [entry, exit] = tradeMarkers([trade(1, "long", 1, 3, 123)], HOUR, COLORS);
    expect(entry).toEqual({
      time: T0 + HOUR, position: "belowBar", shape: "arrowUp", color: "green", text: "Long #1", id: "entry-1",
    });
    expect(exit).toEqual({
      time: T0 + 3 * HOUR, position: "aboveBar", shape: "circle", color: "up", text: "+1.23%", id: "exit-1",
    });
  });

  it("marks a short the other way round, and a loss in the loss colour", () => {
    const [entry, exit] = tradeMarkers([trade(2, "short", 0, 2, -50)], HOUR, COLORS);
    expect([entry.position, entry.shape, entry.color, entry.text]).toEqual(["aboveBar", "arrowDown", "red", "Short #2"]);
    expect([exit.position, exit.color, exit.text]).toEqual(["belowBar", "down", "−0.50%"]);
  });

  it("puts each fill on the bar of the chart's timeframe that holds it", () => {
    const markers = tradeMarkers([trade(1, "long", 1, 6, 10)], 4 * HOUR, COLORS);
    expect(markers.map((m) => m.time)).toEqual([T0, T0 + 4 * HOUR]); // 01:00 → the 00:00 bar, 06:00 → 04:00
  });

  it("keeps time order, an exit before the entry that reverses it", () => {
    const trades = [trade(1, "long", 1, 3, 10), trade(2, "short", 3, 5, -10)];
    const markers = tradeMarkers(trades, HOUR, COLORS);
    expect(markers.map((m) => m.id)).toEqual(["entry-1", "exit-1", "entry-2", "exit-2"]);
    const merged = tradeMarkers(trades, 4 * HOUR, COLORS); // all four on two 4h bars
    expect(merged.map((m) => m.id)).toEqual(["entry-1", "exit-1", "entry-2", "exit-2"]);
    expect(merged.map((m) => m.time)).toEqual([T0, T0, T0, T0 + 4 * HOUR]);
  });

  it("says when the data ended with the trade still open", () => {
    const [, exit] = tradeMarkers([trade(1, "long", 0, 2, 5, { closedByEnd: true })], HOUR, COLORS);
    expect(exit.text).toBe("end +0.05%");
    // Opened on the last bar's open, closed at its close: the entry first.
    const trades = [trade(1, "short", 0, 2, 5), trade(2, "long", 2, 2, 1, { closedByEnd: true })];
    expect(tradeMarkers(trades, HOUR, COLORS).map((m) => m.id)).toEqual(["entry-1", "exit-1", "entry-2", "exit-2"]);
  });
});

describe("markersWithin", () => {
  it("keeps only the markers on loaded bars, so none pile up on the oldest", () => {
    const markers = tradeMarkers([trade(1, "long", 0, 2, 1), trade(2, "long", 5, 9, 1)], HOUR, COLORS);
    expect(markersWithin(markers, T0 + 2 * HOUR, T0 + 5 * HOUR).map((m) => m.id)).toEqual(["exit-1", "entry-2"]);
  });
});

describe("formatReturn", () => {
  it("signs a return and keeps two decimals", () => {
    expect([formatReturn(1.234), formatReturn(-0.5), formatReturn(0)]).toEqual(["+1.23%", "−0.50%", "0.00%"]);
  });
});
