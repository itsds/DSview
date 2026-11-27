/**
 * backtests.js
 *
 * The frontend's one client for backend/routers/backtests.py (Phase 4b):
 * the backtest runs backtest/run_backtest.py stored, each run's trades and
 * its equity curve. Live only, like the routes. Times come back as Unix
 * seconds (lightweight-charts' unit) and amounts as numbers: the backend
 * keeps every digit, the panel shows two.
 *
 * Author: @DS
 */

import { BACKEND_HTTP_URL } from "../config.js";

const toSeconds = (iso) => Math.floor(new Date(iso).getTime() / 1000);
const toNumber = (value) => (value === null || value === undefined ? null : Number(value));

async function getJson(path, params) {
  const query = params ? `?${params}` : "";
  const response = await fetch(`${BACKEND_HTTP_URL}${path}${query}`);
  if (!response.ok) throw new Error(`GET ${path} failed: ${response.status} ${response.statusText}`);
  return response.json();
}

/** A run as GET /backtests lists it: times in seconds, amounts as numbers, counts as they are. */
export function parseRun(run) {
  const stats = {};
  for (const [key, value] of Object.entries(run.stats ?? {})) {
    stats[key] = typeof value === "string" ? Number(value) : value;
  }
  return {
    id: run.run_id,
    createdAt: toSeconds(run.created_at),
    symbol: run.symbol,
    strategy: run.strategy,
    params: run.params ?? {},
    timeframe: run.timeframe,
    firstBar: toSeconds(run.first_bar),
    lastBar: toSeconds(run.last_bar),
    capital: toNumber(run.initial_capital),
    feeRate: toNumber(run.fee_rate),
    stats,
    sparkCheck: run.spark_check ?? null,
    sameAs: run.same_as_run ?? null,
  };
}

export function parseTrade(trade) {
  return {
    number: trade.trade_no,
    side: trade.side,
    entryTime: toSeconds(trade.entry_time),
    entryPrice: Number(trade.entry_price),
    exitTime: toSeconds(trade.exit_time),
    exitPrice: Number(trade.exit_price),
    quantity: Number(trade.quantity),
    fees: Number(trade.fees),
    pnl: Number(trade.pnl),
    returnPct: Number(trade.return_pct),
    bars: trade.bars,
    closedByEnd: trade.closed_by_end,
  };
}

/** `symbol`'s runs, newest first. */
export async function fetchBacktests(symbol) {
  const runs = await getJson("/backtests", new URLSearchParams({ symbol }));
  return runs.map(parseRun);
}

/** One run with its `trades`, oldest first. */
export async function fetchBacktest(runId) {
  const run = await getJson(`/backtests/${runId}`);
  return { ...parseRun(run), trades: run.trades.map(parseTrade) };
}

/** The run's equity curve: about `maxPoints` points of { time, value }, and the full curve's length. */
export async function fetchEquity(runId, maxPoints = 2000) {
  const curve = await getJson(`/backtests/${runId}/equity`, new URLSearchParams({ max_points: String(maxPoints) }));
  return {
    total: curve.total,
    points: curve.points.map(([time, equity]) => ({ time: toSeconds(time), value: Number(equity) })),
  };
}
