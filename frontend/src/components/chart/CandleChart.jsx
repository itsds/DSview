/**
 * CandleChart.jsx
 *
 * Thin wrapper around TradingView's open-source lightweight-charts (v5).
 * `history` (from useCandleHistory.js) seeds the chart once via
 * setData(); `candle` (from useTimeframeCandle.js) keeps it live-updating
 * via series.update() from then on — the standard lightweight-charts
 * pattern for "backfill once, then stream."
 *
 * update() specifically (not setData() again) for live candles: it
 * replaces the bar at a given time, or appends a new one once time has
 * advanced — exactly what's needed since an in-progress candle keeps the
 * same window_start until the next window starts.
 *
 * Scrolling back to near the oldest loaded bar fetches the page before it
 * and prepends it (infinite history).
 *
 * Drawings are drawing/DrawingController.js's job and indicators are
 * indicators/IndicatorManager.js's: both are created with the chart and
 * kept in step with it here — the controller told the active tool and
 * magnet setting, the manager the indicator list and every data change.
 *
 * Mounted once per symbol + timeframe (App.jsx keys it), so a switch gets
 * a fresh chart and fresh refs instead of resetting them by hand.
 *
 * Colours come from chartTheme.js (i.e. styles/theme.css); the
 * O/H/L/C readout is Legend.jsx, fed by the crosshair.
 *
 * Author: @DS
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { CandlestickSeries, HistogramSeries, createChart } from "lightweight-charts";

import { HISTORY_PAGE_SIZE, fetchCandles } from "../../api/candles.js";
import { DrawingController } from "../../drawing/DrawingController.js";
import { IndicatorManager } from "../../indicators/IndicatorManager.js";
import { legendRowsAt } from "../../indicators/legend.js";
import { DAY_SECONDS, candleTime, timeframeSeconds } from "../../lib/timeframes.js";
import styles from "./CandleChart.module.css";
import { candleSeriesOptions, chartOptions, readChartTheme } from "./chartTheme.js";
import PaneLegend from "./IndicatorLegend.jsx";
import Legend from "./Legend.jsx";
import TextInput from "./TextInput.jsx";

// Fetch the previous page once the left edge is this close to the oldest bar.
const LOAD_OLDER_WITHIN_BARS = 10;

function toChartBar(candle) {
  return {
    time: candleTime(candle),
    open: Number(candle.open),
    high: Number(candle.high),
    low: Number(candle.low),
    close: Number(candle.close),
  };
}

function toVolumeBar(candle, theme) {
  return {
    time: candleTime(candle),
    value: Number(candle.volume),
    color: Number(candle.close) >= Number(candle.open) ? theme.upVolume : theme.downVolume,
  };
}

// Whichever of the history tail and the live candle is newer — what the
// legend shows while the crosshair isn't over a bar.
function latestCandle(history, candle) {
  const last = history.length > 0 ? history[history.length - 1] : null;
  if (!candle || !last) return candle ?? last;
  return candleTime(candle) >= candleTime(last) ? candle : last;
}

export default function CandleChart({
  symbol,
  timeframe,
  history,
  candle,
  drawingStore,
  activeTool,
  magnet,
  onToolChange,
  indicators,
  indicatorActions,
}) {
  const theme = useMemo(readChartTheme, []);
  const wrapperRef = useRef(null);
  const containerRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);
  const drawingsRef = useRef(null);
  const indicatorsRef = useRef(null);
  const lastBarTimeRef = useRef(null);
  const loadingOlderRef = useRef(false);
  const reachedStartRef = useRef(false);
  const [hoveredBar, setHoveredBar] = useState(null);
  const [textRequest, setTextRequest] = useState(null);
  const [indicatorValues, setIndicatorValues] = useState(null);
  const [paneTops, setPaneTops] = useState([]);

  useEffect(() => {
    const intraday = timeframeSeconds(timeframe) < DAY_SECONDS;
    const chart = createChart(containerRef.current, chartOptions(theme, { intraday }));

    const candleSeries = chart.addSeries(CandlestickSeries, candleSeriesOptions(theme));
    // Keep candles clear of the volume bars along the bottom.
    candleSeries.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.22 } });

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "", // separate scale from price, so volume doesn't squash the candles
      priceFormat: { type: "volume" },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    chart.subscribeCrosshairMove((param) => {
      const bar = param.point ? param.seriesData.get(candleSeries) : undefined;
      if (!bar || bar.open === undefined) {
        setHoveredBar(null);
        return;
      }
      setHoveredBar({ ...bar, volume: param.seriesData.get(volumeSeries)?.value });
    });

    const drawings = new DrawingController({
      chart,
      series: candleSeries,
      volumeSeries,
      element: wrapperRef.current,
      store: drawingStore,
      theme,
      barSeconds: timeframeSeconds(timeframe),
      onToolChange,
      onTextRequest: (point, done) => setTextRequest({ x: point.x, y: point.y, done }),
    });

    const indicatorManager = new IndicatorManager({
      chart,
      candleSeries,
      volumeSeries,
      theme,
      element: containerRef.current,
      onValues: setIndicatorValues,
      onLayout: setPaneTops,
    });

    let disposed = false;

    // Prepends the page before the oldest loaded bar. Merges with
    // series.data() rather than the original history page, so bars that
    // live updates appended since the chart loaded survive.
    const loadOlder = async () => {
      if (loadingOlderRef.current || reachedStartRef.current) return;
      const loaded = candleSeries.data();
      if (loaded.length === 0) return; // first page not in yet
      loadingOlderRef.current = true;
      try {
        const older = await fetchCandles(symbol, { timeframe, before: loaded[0].time });
        if (disposed) return;
        if (older.length < HISTORY_PAGE_SIZE) reachedStartRef.current = true;
        if (older.length === 0) return;
        candleSeries.setData([...older.map(toChartBar), ...candleSeries.data()]);
        volumeSeries.setData([...older.map((c) => toVolumeBar(c, theme)), ...volumeSeries.data()]);
        indicatorManager.refresh({ full: true });
      } catch (error) {
        console.error("Loading older candles failed:", error);
      } finally {
        loadingOlderRef.current = false;
      }
    };

    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && range.from < LOAD_OLDER_WITHIN_BARS) loadOlder();
    });

    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    drawingsRef.current = drawings;
    indicatorsRef.current = indicatorManager;

    return () => {
      disposed = true;
      drawings.destroy();
      indicatorManager.destroy();
      chart.remove();
    };
  }, [theme, symbol, timeframe, drawingStore, onToolChange]);

  useEffect(() => {
    drawingsRef.current.setTool(activeTool);
  }, [activeTool]);

  useEffect(() => {
    drawingsRef.current.setMagnet(magnet);
  }, [magnet]);

  useEffect(() => {
    indicatorsRef.current.setIndicators(indicators);
  }, [indicators]);

  useEffect(() => {
    if (history.length === 0) return;
    const bars = history.map(toChartBar);
    candleSeriesRef.current.setData(bars);
    volumeSeriesRef.current.setData(history.map((c) => toVolumeBar(c, theme)));
    lastBarTimeRef.current = bars[bars.length - 1].time;
    // A short first page is all the history there is.
    reachedStartRef.current = history.length < HISTORY_PAGE_SIZE;
    indicatorsRef.current.refresh({ full: true });
  }, [history, theme]);

  useEffect(() => {
    if (!candle) return;
    const bar = toChartBar(candle);
    // update() throws on a bar older than the last one, which unmounts the
    // whole app. That happens when a late trade refines an already-superseded
    // window (within candle_windowing.py's watermark) — drop it; the refined
    // value is in Postgres and shows up on the next history load.
    if (lastBarTimeRef.current !== null && bar.time < lastBarTimeRef.current) return;
    candleSeriesRef.current.update(bar);
    volumeSeriesRef.current.update(toVolumeBar(candle, theme));
    lastBarTimeRef.current = bar.time;
    indicatorsRef.current.refresh();
  }, [candle, theme]);

  const latest = latestCandle(history, candle);
  const legendBar = hoveredBar ?? (latest && { ...toChartBar(latest), volume: Number(latest.volume) });
  const indicatorRows = legendRowsAt(indicatorValues, hoveredBar?.time ?? null);
  const oscillatorPanes = [...new Set(indicatorRows.map((row) => row.paneIndex))].filter((pane) => pane > 0);

  return (
    <div ref={wrapperRef} className={styles.wrapper}>
      <div ref={containerRef} className={styles.chart} />
      <Legend
        symbol={symbol}
        timeframe={timeframe}
        bar={legendBar}
        indicators={indicatorRows.filter((row) => row.paneIndex === 0)}
        indicatorActions={indicatorActions}
      />
      {oscillatorPanes.map(
        (pane) =>
          paneTops[pane] != null && (
            <PaneLegend
              key={pane}
              top={paneTops[pane]}
              rows={indicatorRows.filter((row) => row.paneIndex === pane)}
              actions={indicatorActions}
            />
          ),
      )}
      {textRequest && (
        <TextInput
          x={textRequest.x}
          y={textRequest.y}
          onDone={(text) => {
            setTextRequest(null);
            textRequest.done(text);
          }}
        />
      )}
    </div>
  );
}
