/**
 * useBacktests.js
 *
 * The Strategy Tester's data (Phase 4b): the symbol's backtest runs
 * (api/backtests.js), which one is picked, its trades and its equity
 * curve. Runs are stored by backtest/run_backtest.py from the command line,
 * so the list is fetched while the panel is open and again every
 * POLL_MS, and a new run shows up on its own. Every run is kept (kickoff
 * decision 1); the picked one is remembered in localStorage, and while it
 * isn't in the list (a first visit, a run deleted by hand) the newest is
 * shown. `showTrades` (also remembered) says whether the chart marks the
 * picked run's trades while the panel is open.
 *
 * Nothing is fetched while the panel is closed, and a run's trades and
 * curve are fetched once and kept: a stored run never changes.
 *
 * Author: @DS
 */

import { useCallback, useEffect, useRef, useState } from "react";

import { fetchBacktest, fetchBacktests, fetchEquity } from "../api/backtests.js";

const POLL_MS = 15_000;
const STORAGE_KEY = "dsview.backtests";

/** The run to show: the remembered one if it's still listed, else the newest (runs are newest first). */
export function pickRun(runs, rememberedId) {
  if (!runs || runs.length === 0) return null;
  return runs.find((run) => run.id === rememberedId) ?? runs[0];
}

function load() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") ?? {};
    return { runId: Number.isInteger(saved.runId) ? saved.runId : null, showTrades: saved.showTrades !== false };
  } catch {
    return { runId: null, showTrades: true }; // storage blocked, or a corrupt entry
  }
}

export function useBacktests(symbol, open) {
  const [saved, setSaved] = useState(load);
  const [runs, setRuns] = useState(null); // null until the first answer
  const [error, setError] = useState(null);
  const [details, setDetails] = useState({}); // run id -> { run with trades, equity }
  const fetching = useRef(new Set());

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(saved));
    } catch {
      // Storage full or blocked: keep it for this session anyway.
    }
  }, [saved]);

  const refresh = useCallback(async () => {
    try {
      setRuns(await fetchBacktests(symbol));
      setError(null);
    } catch (err) {
      setError(err.message);
    }
  }, [symbol]);

  useEffect(() => {
    if (!open) return undefined;
    let timer;
    let cancelled = false;
    const poll = async () => {
      await refresh();
      if (!cancelled) timer = setTimeout(poll, POLL_MS);
    };
    poll();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [open, refresh]);

  const picked = pickRun(runs, saved.runId);
  const pickedId = picked?.id ?? null;

  useEffect(() => {
    if (!open || pickedId === null || details[pickedId] || fetching.current.has(pickedId)) return;
    fetching.current.add(pickedId);
    Promise.all([fetchBacktest(pickedId), fetchEquity(pickedId)])
      .then(([run, equity]) => setDetails((all) => ({ ...all, [pickedId]: { run, equity } })))
      .catch((err) => setError(err.message))
      .finally(() => fetching.current.delete(pickedId));
  }, [open, pickedId, details]);

  const select = useCallback((runId) => setSaved((s) => ({ ...s, runId })), []);
  const setShowTrades = useCallback((showTrades) => setSaved((s) => ({ ...s, showTrades })), []);

  const detail = pickedId === null ? null : details[pickedId] ?? null;
  return {
    runs,
    error,
    picked,
    trades: detail?.run.trades ?? null,
    equity: detail?.equity ?? null,
    showTrades: saved.showTrades,
    select,
    setShowTrades,
    refresh,
  };
}
