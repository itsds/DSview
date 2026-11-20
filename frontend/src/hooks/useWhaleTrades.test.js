import { describe, expect, it, vi } from "vitest";

import { BACKLOG, addTrade, parseTrade } from "./useWhaleTrades.js";

const trade = (id, exchange = "binance") => ({ exchange, trade_id: String(id), symbol: "BTCUSDT" });

describe("addTrade", () => {
  it("puts each new trade first", () => {
    const trades = [trade(1), trade(2), trade(3)].reduce(addTrade, []);
    expect(trades.map((t) => t.trade_id)).toEqual(["3", "2", "1"]);
  });

  it("ignores a trade it already has, so a reconnect's replay adds only what was missed", () => {
    const shown = [trade(1), trade(2)].reduce(addTrade, []);
    const afterReplay = [trade(1), trade(2), trade(3)].reduce(addTrade, shown);
    expect(afterReplay.map((t) => t.trade_id)).toEqual(["3", "2", "1"]);
    expect(addTrade(afterReplay, trade(2))).toBe(afterReplay); // unchanged, no re-render
  });

  it("tells exchanges apart", () => {
    expect([trade(1), trade(1, "kraken")].reduce(addTrade, [])).toHaveLength(2);
  });

  it("keeps the newest BACKLOG trades", () => {
    const trades = Array.from({ length: BACKLOG + 5 }, (_, i) => trade(i)).reduce(addTrade, []);
    expect(trades).toHaveLength(BACKLOG);
    expect(trades[0].trade_id).toBe(String(BACKLOG + 4));
  });
});

describe("parseTrade", () => {
  it("passes an alert object through", () => {
    expect(parseTrade('{"exchange":"binance","trade_id":"1"}')).toEqual({ exchange: "binance", trade_id: "1" });
  });

  it("ignores anything that isn't a JSON object instead of throwing", () => {
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    for (const bad of ["not json", "[1, 2]", "null", "42", '"text"']) {
      expect(parseTrade(bad)).toBeNull();
    }
    expect(logged).toHaveBeenCalledTimes(5);
    logged.mockRestore();
  });
});
