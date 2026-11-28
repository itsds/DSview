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
 * Spark's indicator values (indicators/server.js) arrive with every page
 * of candles and, for the bar in progress, from useLiveIndicators.js; they
 * go into one store per chart, which the manager reads on every refresh.
 * Binance Futures' long/short ratios (indicators/futuresRatios.js) have a
 * store of their own, filled only while a ratio indicator is shown: over
 * REST for the bars loaded (the first page, then each scroll-back page),
 * then live from useLiveFuturesRatios.js. Live's always, replay or not.
 *
 * The volume footprint is footprint/FootprintManager.js's: created with the
 * chart on a footprint timeframe (1m to 1h), turned on and off with the
 * chart type, told the row size setting and every live minute from
 * useLiveFootprint.js; the legend says what it's showing.
 *
 * Mounted once per symbol + timeframe + feed (App.jsx keys it), so a
 * switch gets a fresh chart and fresh refs instead of resetting them by
 * hand. `pipeline` is whose data every fetch and live socket asks for:
 * live's, or a replay's (lib/pipelines.js, Phase 4a).
 *
 * `backtestTrades` are the picked backtest run's trades (Phase 4b), marked
 * on the candles by backtest/BacktestMarkers.js on whatever timeframe the
 * chart shows, and kept to the bars loaded as scroll-back adds more; null
 * for none.
 *
 * With `onPickReplayStart` set, the chart is choosing where a replay starts:
 * the next click on a bar hands that bar's time to it (App.jsx then starts
 * the replay), with a note over the chart saying so.
 *
 * Colours come from chartTheme.js (i.e. styles/theme.css); the
 * O/H/L/C readout is Legend.jsx, fed by the crosshair.
 *
 * Author: @DS
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { CandlestickSeries, HistogramSeries, createChart } from "lightweight-charts";

import { HISTORY_PAGE_SIZE, fetchCandles } from "../../api/candles.js";
import { BacktestMarkers } from "../../backtest/BacktestMarkers.js";
import { DrawingController } from "../../drawing/DrawingController.js";
import { FootprintManager } from "../../footprint/FootprintManager.js";
import { FOOTPRINT_TIMEFRAMES } from "../../footprint/levels.js";
import { useLiveFootprint } from "../../hooks/useLiveFootprint.js";
import { useLiveFuturesRatios } from "../../hooks/useLiveFuturesRatios.js";
import { useLiveIndicators } from "../../hooks/useLiveIndicators.js";
import { LIVE } from "../../lib/pipelines.js";
import { IndicatorManager } from "../../indicators/IndicatorManager.js";
import { usesFuturesRatios } from "../../indicators/definitions.js";
import { createFuturesRatioStore, ratioTimeframe } from "../../indicators/futuresRatios.js";
import { legendRowsAt } from "../../indicators/legend.js";
import { createServerIndicatorStore, serverColumn } from "../../indicators/server.js";
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

// Asks the ratio store for every bar the chart has loaded, through the bar in progress.
function ensureRatios(candleSeries, ratioValues, timeframe) {
  const bars = candleSeries.data();
  if (bars.length === 0) return;
  ratioValues.ensure(bars[0].time, bars[bars.length - 1].time + timeframeSeconds(timeframe));
}

// The legend's line for the footprint, from FootprintManager's status.
function footprintNote(chartType, available, status) {
  if (chartType !== "footprint") return null;
  if (!available) return "Footprint: 1m to 1h only";
  if (!status || status.state === "zoom") return "Footprint: zoom in to see it";
  if (status.state === "price") return "Footprint: price range too wide, zoom in on prices";
  if (status.state === "on") return "Footprint · $" + status.rowSize + " rows";
  return null;
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
  chartType,
  footprintRows,
  pipeline = LIVE,
  onPickReplayStart = null,
  backtestTrades = null,
}) {
  const theme = useMemo(readChartTheme, []);
  const wrapperRef = useRef(null);
  const chartRef = useRef(null);
  const containerRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);
  const drawingsRef = useRef(null);
  const indicatorsRef = useRef(null);
  const serverValuesRef = useRef(null);
  const ratioValuesRef = useRef(null);
  const showsRatiosRef = useRef(false);
  const footprintRef = useRef(null);
  const backtestMarkersRef = useRef(null);
  const lastBarTimeRef = useRef(null);
  const historyLoadedRef = useRef(false);
  const loadingOlderRef = useRef(false);
  const reachedStartRef = useRef(false);
  const [hoveredBar, setHoveredBar] = useState(null);
  const [textRequest, setTextRequest] = useState(null);
  const [indicatorValues, setIndicatorValues] = useState(null);
  const [paneTops, setPaneTops] = useState([]);
  const [footprintStatus, setFootprintStatus] = useState(null);
  const footprintAvailable = FOOTPRINT_TIMEFRAMES.includes(timeframe);

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
      symbol,
      pipeline,
      barSeconds: timeframeSeconds(timeframe),
      onToolChange,
      onTextRequest: (point, done) => setTextRequest({ x: point.x, y: point.y, done }),
    });

    const serverValues = createServerIndicatorStore();
    const ratioValues = createFuturesRatioStore({
      symbol,
      timeframe,
      onChange: () => indicatorManager.refresh({ full: true }),
    });
    const indicatorManager = new IndicatorManager({
      chart,
      candleSeries,
      volumeSeries,
      serverValues,
      ratioValues,
      theme,
      element: containerRef.current,
      onValues: setIndicatorValues,
      onLayout: setPaneTops,
    });

    const footprint = FOOTPRINT_TIMEFRAMES.includes(timeframe)
      ? new FootprintManager({ chart, candleSeries, symbol, timeframe, theme, pipeline, onStatus: setFootprintStatus })
      : null;

    const backtestMarkers = new BacktestMarkers({ candleSeries, barSeconds: timeframeSeconds(timeframe), theme });

    let disposed = false;

    // Prepends the page before the oldest loaded bar. Merges with
    // series.data() rather than the original history page, so bars that
    // live updates appended since the chart loaded survive.
    //
    // Only once the first page is in: the live candle can reach the chart
    // before it, and paging back from that lone bar fetched the very page
    // still loading, which then landed after it out of order (setData()
    // throws on that). For the same reason, only bars older than the
    // chart's first are kept.
    const loadOlder = async () => {
      if (!historyLoadedRef.current || loadingOlderRef.current || reachedStartRef.current) return;
      const loaded = candleSeries.data();
      if (loaded.length === 0) return;
      loadingOlderRef.current = true;
      try {
        const older = await fetchCandles(symbol, { timeframe, before: loaded[0].time, pipeline });
        if (disposed) return;
        if (older.length < HISTORY_PAGE_SIZE) reachedStartRef.current = true;
        const first = candleSeries.data()[0].time;
        const page = older.filter((candle) => candleTime(candle) < first);
        if (page.length === 0) return;
        serverValues.addHistory(page);
        candleSeries.setData([...page.map(toChartBar), ...candleSeries.data()]);
        volumeSeries.setData([...page.map((c) => toVolumeBar(c, theme)), ...volumeSeries.data()]);
        indicatorManager.refresh({ full: true });
        backtestMarkers.refresh();
        if (showsRatiosRef.current) ensureRatios(candleSeries, ratioValues, timeframe);
      } catch (error) {
        console.error("Loading older candles failed:", error);
      } finally {
        loadingOlderRef.current = false;
      }
    };

    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && range.from < LOAD_OLDER_WITHIN_BARS) loadOlder();
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    drawingsRef.current = drawings;
    indicatorsRef.current = indicatorManager;
    serverValuesRef.current = serverValues;
    ratioValuesRef.current = ratioValues;
    footprintRef.current = footprint;
    backtestMarkersRef.current = backtestMarkers;

    return () => {
      disposed = true;
      ratioValues.destroy();
      drawings.destroy();
      indicatorManager.destroy();
      footprint?.destroy();
      backtestMarkers.destroy();
      chart.remove();
    };
  }, [theme, symbol, timeframe, drawingStore, onToolChange, pipeline]);

  useEffect(() => {
    drawingsRef.current.setTool(activeTool);
  }, [activeTool]);

  useEffect(() => {
    drawingsRef.current.setMagnet(magnet);
  }, [magnet]);

  const showsRatios = usesFuturesRatios(indicators);
  useEffect(() => {
    indicatorsRef.current.setIndicators(indicators);
    showsRatiosRef.current = showsRatios;
    if (showsRatios && historyLoadedRef.current) ensureRatios(candleSeriesRef.current, ratioValuesRef.current, timeframe);
  }, [indicators, showsRatios, timeframe]);

  useLiveFuturesRatios(symbol, ratioTimeframe(timeframe), showsRatios, (update) => {
    ratioValuesRef.current.addLive(update);
    // On 1m a period lands on the five bars before the newest: refresh them all.
    indicatorsRef.current.refresh({ full: true });
  });

  useEffect(() => {
    backtestMarkersRef.current.setTrades(backtestTrades);
  }, [backtestTrades]);

  const showsFootprint = chartType === "footprint" && footprintAvailable;
  useEffect(() => {
    footprintRef.current?.setEnabled(showsFootprint);
  }, [showsFootprint]);

  useEffect(() => {
    footprintRef.current?.setRowSize(footprintRows);
  }, [footprintRows]);

  useLiveFootprint(symbol, showsFootprint, (update) => footprintRef.current?.addLive(update), pipeline);

  // Choosing a replay's start: the next click on a bar.
  useEffect(() => {
    if (!onPickReplayStart) return undefined;
    const chart = chartRef.current;
    const onClick = (param) => {
      if (param.time !== undefined) onPickReplayStart(param.time);
    };
    chart.subscribeClick(onClick);
    return () => chart.unsubscribeClick(onClick);
  }, [onPickReplayStart]);

  const showsServerIndicators = indicators.some((indicator) => serverColumn(indicator.type, indicator.params));
  useLiveIndicators(
    symbol,
    timeframe,
    showsServerIndicators,
    (update) => {
      serverValuesRef.current.addLive(update);
      // A revision of an older bar changes a point mid-line, which update() can't.
      const last = lastBarTimeRef.current;
      indicatorsRef.current.refresh({ full: last !== null && update.time < last });
    },
    pipeline,
  );

  useEffect(() => {
    if (history.length === 0) return;
    const bars = history.map(toChartBar);
    serverValuesRef.current.addHistory(history);
    candleSeriesRef.current.setData(bars);
    volumeSeriesRef.current.setData(history.map((c) => toVolumeBar(c, theme)));
    lastBarTimeRef.current = bars[bars.length - 1].time;
    historyLoadedRef.current = true;
    // A short first page is all the history there is.
    reachedStartRef.current = history.length < HISTORY_PAGE_SIZE;
    indicatorsRef.current.refresh({ full: true });
    backtestMarkersRef.current.refresh();
    if (showsRatiosRef.current) ensureRatios(candleSeriesRef.current, ratioValuesRef.current, timeframe);
  }, [history, theme, timeframe]);

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
    <div ref={wrapperRef} className={onPickReplayStart ? `${styles.wrapper} ${styles.picking}` : styles.wrapper}>
      <div ref={containerRef} className={styles.chart} />
      {onPickReplayStart && (
        <div className={styles.pickNote} role="status">
          Click the bar to replay from · Esc to cancel
        </div>
      )}
      <Legend
        symbol={symbol}
        timeframe={timeframe}
        bar={legendBar}
        indicators={indicatorRows.filter((row) => row.paneIndex === 0)}
        indicatorActions={indicatorActions}
        note={footprintNote(chartType, footprintAvailable, footprintStatus)}
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
