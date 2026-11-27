/**
 * EquityChart.jsx
 *
 * A backtest run's equity curve in the Strategy Tester (Phase 4b): a
 * lightweight-charts baseline series at the starting capital, in the up
 * colour above it and the down colour below, one point per bar's close
 * (thinned by the backend for long runs, keeping every trough and peak).
 * A chart of its own, mounted per run (StrategyTester keys it).
 *
 * Author: @DS
 */

import { useEffect, useMemo, useRef } from "react";
import { BaselineSeries, createChart } from "lightweight-charts";

import { chartOptions, readChartTheme } from "../chart/chartTheme.js";
import styles from "./StrategyTester.module.css";

export default function EquityChart({ points, capital, intraday }) {
  const theme = useMemo(readChartTheme, []);
  const containerRef = useRef(null);

  useEffect(() => {
    const options = chartOptions(theme, { intraday });
    const chart = createChart(containerRef.current, {
      ...options,
      // Down to a twentieth of a pixel per point, so fitContent() can show
      // the whole curve (up to the backend's ~2,000 points); at the default
      // 0.5 px it would keep only the newest stretch that fits.
      timeScale: { ...options.timeScale, rightOffset: 2, minBarSpacing: 0.05 },
      handleScroll: false,
      handleScale: false,
    });
    const series = chart.addSeries(BaselineSeries, {
      baseValue: { type: "price", price: capital },
      topLineColor: theme.up,
      topFillColor1: theme.upSoft,
      topFillColor2: theme.upSoft,
      bottomLineColor: theme.down,
      bottomFillColor1: theme.downSoft,
      bottomFillColor2: theme.downSoft,
      lineWidth: 2,
      priceLineVisible: false,
      priceFormat: { type: "price", precision: 0, minMove: 1 },
    });
    series.setData(points);
    chart.timeScale().fitContent();
    return () => chart.remove();
  }, [theme, points, capital, intraday]);

  return <div ref={containerRef} className={styles.equity} />;
}
