import { describe, expect, it } from "vitest";

import { legendRowsAt } from "./legend.js";

const source = {
  indexByTime: new Map([
    [100, 0],
    [200, 1],
    [300, 2],
  ]),
  lastIndex: 2,
  indicators: [
    {
      id: "r",
      label: "Taker buy/sell",
      paneIndex: 1,
      visible: true,
      hasSettings: false,
      fromServer: false,
      source: "Binance Futures",
      precision: 3,
      lines: [{ key: "value", label: "Ratio", values: [1.1, 1.2, null], color: "#f90", dashed: false }],
    },
  ],
};

describe("legendRowsAt", () => {
  it("shows the hovered bar's value, even when it has none", () => {
    expect(legendRowsAt(source, 200)[0].values[0].value).toBe(1.2);
    expect(legendRowsAt(source, 300)[0].values[0].value).toBeNull();
  });

  it("off the chart, shows the latest value when the bar in progress has none yet", () => {
    const [row] = legendRowsAt(source, null);
    expect(row.values[0].value).toBe(1.2);
    expect([row.source, row.precision]).toEqual(["Binance Futures", 3]);
  });

  it("shows nothing for a line with no value at all", () => {
    const empty = { ...source, indicators: [{ ...source.indicators[0], lines: [{ ...source.indicators[0].lines[0], values: [null, null, null] }] }] };
    expect(legendRowsAt(empty, null)[0].values[0].value).toBeNull();
  });
});
