/**
 * App.jsx
 *
 * Root component — lays out the workspace (top bar, drawing toolbar,
 * chart, right panel, bottom bar), holds the UI state (timeframe, chart
 * type and footprint rows, active drawing tool, magnet, indicators and
 * their dialogs), and feeds the chart
 * its data: useCandleHistory.js backfills over REST, useLiveCandles.js
 * streams 1m candles over the WebSocket, and useTimeframeCandle.js rolls
 * that stream up to the selected timeframe; useWhaleTrades.js feeds the
 * right panel's whale list. During a replay (Phase 4a) all of them, the
 * chart's own fetches and sockets included, follow the replay's feed
 * instead of live's (lib/pipelines.js): useReplay.js polls the replay
 * service, the top bar's Replay button picks the bar to start from, and
 * ReplayBar.jsx steers it. The bottom bar opens the Strategy Tester
 * (Phase 4b) under the chart: backtest runs from useBacktests.js, the
 * picked run's trades marked on the chart while it's open. Drawings live in a per-symbol
 * drawing store and indicators in useIndicators.js, both outliving chart
 * remounts, so they survive timeframe switches. The chart sits in an
 * ErrorBoundary so a chart error can't blank the whole page. Symbol
 * selection is still future work.
 *
 * Author: @DS
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import styles from "./App.module.css";
import ErrorBoundary from "./components/ErrorBoundary.jsx";
import StrategyTester from "./components/backtest/StrategyTester.jsx";
import CandleChart from "./components/chart/CandleChart.jsx";
import IndicatorSettingsDialog from "./components/indicators/IndicatorSettingsDialog.jsx";
import IndicatorsDialog from "./components/indicators/IndicatorsDialog.jsx";
import BottomBar from "./components/layout/BottomBar.jsx";
import DrawingToolbar from "./components/layout/DrawingToolbar.jsx";
import RightPanel from "./components/layout/RightPanel.jsx";
import TopBar from "./components/layout/TopBar.jsx";
import ReplayBar from "./components/replay/ReplayBar.jsx";
import { createDrawingStore } from "./drawing/drawingStore.js";
import { CROSSHAIR_TOOL } from "./drawing/tools.js";
import { useBacktests } from "./hooks/useBacktests.js";
import { useCandleHistory } from "./hooks/useCandleHistory.js";
import { useChartSettings } from "./hooks/useChartSettings.js";
import { useIndicators } from "./hooks/useIndicators.js";
import { useLiveCandles } from "./hooks/useLiveCandles.js";
import { useReplay } from "./hooks/useReplay.js";
import { useTimeframeCandle } from "./hooks/useTimeframeCandle.js";
import { useWhaleTrades } from "./hooks/useWhaleTrades.js";
import { REPLAY, feedFor, replayActive } from "./lib/pipelines.js";

const SYMBOL = "BTCUSDT";
const DEFAULT_TIMEFRAME = "1m";
const TESTER_KEY = "dsview.strategyTesterOpen";

function savedTesterOpen() {
  try {
    return localStorage.getItem(TESTER_KEY) === "true";
  } catch {
    return false;
  }
}

export default function App() {
  const [timeframe, setTimeframe] = useState(DEFAULT_TIMEFRAME);
  const [activeTool, setActiveTool] = useState(CROSSHAIR_TOOL);
  const [magnet, setMagnet] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const drawingStore = useMemo(() => createDrawingStore(SYMBOL), []);
  const indicators = useIndicators();
  const chartSettings = useChartSettings();
  const replay = useReplay();
  const [pickingReplayStart, setPickingReplayStart] = useState(false);
  const feed = feedFor(replay.status, SYMBOL);
  const history = useCandleHistory(SYMBOL, timeframe, feed);
  const minuteCandle = useLiveCandles(SYMBOL, feed);
  const candle = useTimeframeCandle(SYMBOL, timeframe, minuteCandle, feed);
  const whales = useWhaleTrades(feed);
  const [testerOpen, setTesterOpen] = useState(savedTesterOpen);
  const backtests = useBacktests(SYMBOL, testerOpen);
  const backtestTrades = testerOpen && backtests.showTrades ? backtests.trades : null;

  useEffect(() => {
    try {
      localStorage.setItem(TESTER_KEY, String(testerOpen));
    } catch {
      // Storage blocked: the panel just won't reopen after a reload.
    }
  }, [testerOpen]);

  // The chart hands the clicked bar's time here while a start is being picked.
  const startReplayRef = useRef(replay.start);
  startReplayRef.current = replay.start;
  const pickReplayStart = useCallback((time) => {
    setPickingReplayStart(false);
    startReplayRef.current(SYMBOL, time);
  }, []);

  useEffect(() => {
    if (!pickingReplayStart) return undefined;
    setActiveTool(CROSSHAIR_TOOL); // a click picks the bar, rather than placing a drawing
    const onKey = (event) => {
      if (event.key === "Escape") setPickingReplayStart(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pickingReplayStart]);

  const editing = indicators.indicators.find((indicator) => indicator.id === editingId) ?? null;

  const clearDrawings = () => {
    // There's no undo yet, so ask first.
    if (drawingStore.getAll().length > 0 && window.confirm(`Remove all ${SYMBOL} drawings?`)) {
      drawingStore.clear();
    }
  };

  const indicatorActions = {
    toggle: indicators.toggle,
    settings: setEditingId,
    remove: indicators.remove,
  };

  return (
    <div className={testerOpen ? `${styles.app} ${styles.withTester}` : styles.app}>
      <div className={styles.top}>
        <TopBar
          symbol={SYMBOL}
          timeframe={timeframe}
          onTimeframeChange={setTimeframe}
          onIndicatorsClick={() => setPickerOpen(true)}
          chartSettings={chartSettings}
          replay={{
            available: replay.available,
            active: replayActive(replay.status),
            picking: pickingReplayStart,
            onClick: () => setPickingReplayStart((picking) => !picking),
          }}
        />
      </div>
      <div className={styles.tools}>
        <DrawingToolbar
          activeTool={activeTool}
          onToolChange={setActiveTool}
          magnet={magnet}
          onMagnetChange={setMagnet}
          onClearDrawings={clearDrawings}
        />
      </div>
      <main className={styles.chart}>
        {/* Keyed so a switch (of symbol, timeframe, or live/replay run) mounts a fresh chart and clears any error. */}
        <ErrorBoundary key={`${SYMBOL}|${timeframe}|${feed.key}`} name="chart">
          <CandleChart
            symbol={SYMBOL}
            timeframe={timeframe}
            history={history}
            candle={candle}
            drawingStore={drawingStore}
            activeTool={activeTool}
            magnet={magnet}
            onToolChange={setActiveTool}
            indicators={indicators.indicators}
            indicatorActions={indicatorActions}
            chartType={chartSettings.chartType}
            footprintRows={chartSettings.footprintRows}
            pipeline={feed.pipeline}
            onPickReplayStart={pickingReplayStart ? pickReplayStart : null}
            backtestTrades={backtestTrades}
          />
        </ErrorBoundary>
        {replayActive(replay.status) && (
          <ReplayBar
            status={replay.status}
            speed={replay.speed}
            error={replay.error}
            onPlay={replay.resume}
            onPause={replay.pause}
            onSpeed={replay.setSpeed}
            onSelectBar={() => setPickingReplayStart(true)}
            onExit={() => {
              setPickingReplayStart(false);
              replay.stop();
            }}
          />
        )}
      </main>
      <div className={styles.panel}>
        <RightPanel
          symbol={SYMBOL}
          candle={minuteCandle}
          whaleTrades={whales.trades}
          whaleStatus={whales.status}
          replaying={feed.pipeline === REPLAY}
        />
      </div>
      {testerOpen && (
        <div className={styles.tester}>
          <ErrorBoundary name="Strategy Tester">
            <StrategyTester backtests={backtests} onClose={() => setTesterOpen(false)} />
          </ErrorBoundary>
        </div>
      )}
      <div className={styles.bottom}>
        <BottomBar testerOpen={testerOpen} onTesterToggle={() => setTesterOpen((open) => !open)} />
      </div>
      {pickerOpen && (
        <IndicatorsDialog onAdd={indicators.add} canAdd={indicators.canAdd} onClose={() => setPickerOpen(false)} />
      )}
      {editing && (
        <IndicatorSettingsDialog
          key={editing.id}
          indicator={editing}
          onSave={(params) => indicators.setParams(editing.id, params)}
          onClose={() => setEditingId(null)}
        />
      )}
    </div>
  );
}
