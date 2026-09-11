/**
 * CandleChart.jsx
 *
 * Thin wrapper around TradingView's open-source lightweight-charts.
 * `history` (from useCandleHistory.js) seeds the chart once via
 * setData(); `candle` (from useLiveCandles.js) keeps it live-updating
 * via series.update() from then on — the standard lightweight-charts
 * pattern for "backfill once, then stream."
 *
 * update() specifically (not setData() again) for live candles: it
 * replaces the bar at a given time, or appends a new one once time has
 * advanced — exactly what's needed since the backend repeats the same
 * window_start (in-progress candle) until a new 1-minute window starts
 * server-side.
 *
 * Author: @DS
 */

import { useEffect, useRef } from "react";
import { createChart, ColorType } from "lightweight-charts";

function toChartBar(candle) {
  return {
    time: Math.floor(new Date(candle.window_start).getTime() / 1000),
    open: Number(candle.open),
    high: Number(candle.high),
    low: Number(candle.low),
    close: Number(candle.close),
  };
}

function toVolumeBar(candle) {
  return {
    time: Math.floor(new Date(candle.window_start).getTime() / 1000),
    value: Number(candle.volume),
    color: Number(candle.close) >= Number(candle.open) ? "#26a69a" : "#ef5350",
  };
}

export default function CandleChart({ history, candle }) {
  const containerRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);

  useEffect(() => {
    const chart = createChart(containerRef.current, {
      layout: { background: { type: ColorType.Solid, color: "#ffffff" } },
      width: containerRef.current.clientWidth,
      height: 500,
    });

    candleSeriesRef.current = chart.addCandlestickSeries();

    volumeSeriesRef.current = chart.addHistogramSeries({
      priceScaleId: "", // separate scale from price, so volume doesn't squash the candles
      priceFormat: { type: "volume" },
    });
    volumeSeriesRef.current.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    const handleResize = () => chart.applyOptions({ width: containerRef.current.clientWidth });
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, []);

  useEffect(() => {
    if (!history || history.length === 0) return;
    candleSeriesRef.current.setData(history.map(toChartBar));
    volumeSeriesRef.current.setData(history.map(toVolumeBar));
  }, [history]);

  useEffect(() => {
    if (!candle) return;
    candleSeriesRef.current.update(toChartBar(candle));
    volumeSeriesRef.current.update(toVolumeBar(candle));
  }, [candle]);

  return <div ref={containerRef} />;
}
