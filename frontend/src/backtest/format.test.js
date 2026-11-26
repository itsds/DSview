import { describe, expect, it } from "vitest";

import { parseRun, parseTrade } from "../api/backtests.js";
import { pickRun } from "../hooks/useBacktests.js";
import {
  formatMoney,
  formatPct,
  formatRatio,
  formatSignedMoney,
  formatTime,
  runLabel,
  sparkCheckNote,
} from "./format.js";

// A run as GET /backtests returns it (backend/routers/backtests.py).
const RUN = {
  run_id: 12,
  created_at: "2026-11-26T10:00:00.123456+00:00",
  symbol: "BTCUSDT",
  strategy: "ema_cross",
  params: { fast: 20, slow: 50 },
  timeframe: "1h",
  first_bar: "2025-11-26T00:00:00+00:00",
  last_bar: "2026-11-26T09:00:00+00:00",
  initial_capital: "10000",
  fee_rate: "0.001",
  stats: { net_profit: "-1234.5678", total_trades: 17, profit_factor: null, win_rate_pct: "41.17647058823529411764705882" },
  spark_check: { bars: 8770, equal: 8770, differ: 0, missing: 0, first_difference: null },
  same_as_run: 11,
};

describe("parseRun / parseTrade", () => {
  it("turns times into Unix seconds and amounts into numbers, keeping counts and nulls", () => {
    const run = parseRun(RUN);
    expect(run).toMatchObject({ id: 12, timeframe: "1h", capital: 10000, feeRate: 0.001, sameAs: 11 });
    expect(run.firstBar).toBe(Date.UTC(2025, 10, 26) / 1000);
    expect(run.createdAt).toBe(Date.UTC(2026, 10, 26, 10) / 1000);
    expect(run.stats).toEqual({ net_profit: -1234.5678, total_trades: 17, profit_factor: null, win_rate_pct: Number("41.17647058823529411764705882") });
    expect(parseRun({ ...RUN, same_as_run: null, spark_check: null })).toMatchObject({ sameAs: null, sparkCheck: null });
  });

  it("parses a trade", () => {
    const trade = parseTrade({
      trade_no: 3, side: "short", entry_time: "2026-01-01T05:00:00+00:00", entry_price: "97000.1",
      exit_time: "2026-01-01T09:00:00+00:00", exit_price: "96000", quantity: "0.103", fees: "19.3",
      pnl: "83.6", return_pct: "0.84", bars: 4, closed_by_end: false,
    });
    expect(trade).toEqual({
      number: 3, side: "short", entryTime: Date.UTC(2026, 0, 1, 5) / 1000, entryPrice: 97000.1,
      exitTime: Date.UTC(2026, 0, 1, 9) / 1000, exitPrice: 96000, quantity: 0.103, fees: 19.3,
      pnl: 83.6, returnPct: 0.84, bars: 4, closedByEnd: false,
    });
  });
});

describe("pickRun", () => {
  const runs = [{ id: 3 }, { id: 2 }, { id: 1 }]; // newest first
  it("shows the remembered run while it's listed, else the newest", () => {
    expect(pickRun(runs, 2)).toBe(runs[1]);
    expect(pickRun(runs, 9)).toBe(runs[0]);
    expect(pickRun(runs, null)).toBe(runs[0]);
    expect(pickRun([], 2)).toBeNull();
    expect(pickRun(null, 2)).toBeNull();
  });
});

describe("format", () => {
  it("writes money, percentages and ratios, a dash for what doesn't exist", () => {
    expect([formatMoney(1234.567), formatMoney(-0.5), formatMoney(null)]).toEqual(["$1,234.57", "−$0.50", "—"]);
    expect([formatSignedMoney(12), formatSignedMoney(-12), formatSignedMoney(0)]).toEqual(["+$12.00", "−$12.00", "$0.00"]);
    expect([formatPct(41.176), formatPct(-3.5, { signed: true }), formatPct(2, { signed: true })]).toEqual([
      "41.18%", "−3.50%", "+2.00%",
    ]);
    expect([formatRatio(1.23456), formatRatio(null)]).toEqual(["1.235", "—"]);
    expect(formatTime(Date.UTC(2026, 10, 20, 10, 5) / 1000)).toBe("2026-11-20 10:05");
  });

  it("names a run by its number, strategy, parameters, timeframe and span", () => {
    expect(runLabel(parseRun(RUN))).toBe("#12 · ema_cross 20/50 · 1h · 2025-11-26 → 2026-11-26");
  });

  it("says what the check against Spark found", () => {
    expect(sparkCheckNote(RUN.spark_check)).toEqual({ text: "EMAs equal Spark's on 8,770 of 8,770 bars", ok: true });
    expect(sparkCheckNote({ ...RUN.spark_check, equal: 8769, differ: 1 }).ok).toBe(false);
    expect(sparkCheckNote({ ...RUN.spark_check, equal: 8760, missing: 10 })).toEqual({
      text: "EMAs equal Spark's on 8,760 of 8,770 bars; 10 not computed by Spark yet",
      ok: null,
    });
    expect(sparkCheckNote(null).ok).toBeNull();
  });
});
