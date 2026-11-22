/**
 * useChartSettings.js
 *
 * The chart type ("candles" or "footprint") and the footprint's row size
 * ("auto", or a fixed size in dollars: TradingView's "ticks per row"),
 * persisted to localStorage so a reload keeps them. Both outlive chart
 * remounts, so they survive timeframe switches; on 4h and 1D, which have
 * no footprint, the chart shows candles whatever the type.
 *
 * Author: @DS
 */

import { useCallback, useEffect, useState } from "react";

import { ROW_SIZES } from "../footprint/levels.js";

const STORAGE_KEY = "dsview.chartSettings";
export const CHART_TYPES = ["candles", "footprint"];
const DEFAULTS = { chartType: "candles", footprintRows: "auto" };

function load() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") ?? {};
    return {
      chartType: CHART_TYPES.includes(saved.chartType) ? saved.chartType : DEFAULTS.chartType,
      footprintRows: ROW_SIZES.includes(saved.footprintRows) ? saved.footprintRows : DEFAULTS.footprintRows,
    };
  } catch {
    return { ...DEFAULTS }; // storage blocked, or a corrupt entry
  }
}

export function useChartSettings() {
  const [settings, setSettings] = useState(load);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
    } catch {
      // Storage full or blocked: keep them for this session anyway.
    }
  }, [settings]);

  const setChartType = useCallback((chartType) => setSettings((s) => ({ ...s, chartType })), []);
  const setFootprintRows = useCallback((footprintRows) => setSettings((s) => ({ ...s, footprintRows })), []);

  return { ...settings, setChartType, setFootprintRows };
}
