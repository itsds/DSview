/**
 * App.jsx
 *
 * Root component — lays out the workspace (top bar, drawing toolbar,
 * chart, right panel, bottom bar), holds the UI state (timeframe, active
 * drawing tool, magnet, indicators and their dialogs), and feeds the chart
 * its data: useCandleHistory.js backfills over REST, useLiveCandles.js
 * streams 1m candles over the WebSocket, and useTimeframeCandle.js rolls
 * that stream up to the selected timeframe. Drawings live in a per-symbol
 * drawing store and indicators in useIndicators.js, both outliving chart
 * remounts, so they survive timeframe switches. The chart sits in an
 * ErrorBoundary so a chart error can't blank the whole page. Symbol
 * selection is still future work.
 *
 * Author: @DS
 */

import { useMemo, useState } from "react";

import styles from "./App.module.css";
import ErrorBoundary from "./components/ErrorBoundary.jsx";
import CandleChart from "./components/chart/CandleChart.jsx";
import IndicatorSettingsDialog from "./components/indicators/IndicatorSettingsDialog.jsx";
import IndicatorsDialog from "./components/indicators/IndicatorsDialog.jsx";
import BottomBar from "./components/layout/BottomBar.jsx";
import DrawingToolbar from "./components/layout/DrawingToolbar.jsx";
import RightPanel from "./components/layout/RightPanel.jsx";
import TopBar from "./components/layout/TopBar.jsx";
import { createDrawingStore } from "./drawing/drawingStore.js";
import { CROSSHAIR_TOOL } from "./drawing/tools.js";
import { useCandleHistory } from "./hooks/useCandleHistory.js";
import { useIndicators } from "./hooks/useIndicators.js";
import { useLiveCandles } from "./hooks/useLiveCandles.js";
import { useTimeframeCandle } from "./hooks/useTimeframeCandle.js";

const SYMBOL = "BTCUSDT";
const DEFAULT_TIMEFRAME = "1m";

export default function App() {
  const [timeframe, setTimeframe] = useState(DEFAULT_TIMEFRAME);
  const [activeTool, setActiveTool] = useState(CROSSHAIR_TOOL);
  const [magnet, setMagnet] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const drawingStore = useMemo(() => createDrawingStore(SYMBOL), []);
  const indicators = useIndicators();
  const history = useCandleHistory(SYMBOL, timeframe);
  const minuteCandle = useLiveCandles(SYMBOL);
  const candle = useTimeframeCandle(SYMBOL, timeframe, minuteCandle);

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
    <div className={styles.app}>
      <div className={styles.top}>
        <TopBar
          symbol={SYMBOL}
          timeframe={timeframe}
          onTimeframeChange={setTimeframe}
          onIndicatorsClick={() => setPickerOpen(true)}
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
        {/* Keyed so a switch mounts a fresh chart (and clears any error). */}
        <ErrorBoundary key={`${SYMBOL}|${timeframe}`} name="chart">
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
          />
        </ErrorBoundary>
      </main>
      <div className={styles.panel}>
        <RightPanel symbol={SYMBOL} candle={minuteCandle} />
      </div>
      <div className={styles.bottom}>
        <BottomBar />
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
