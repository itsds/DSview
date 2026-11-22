/**
 * TopBar.jsx
 *
 * Symbol, timeframes, chart type, indicators, and chart actions along the
 * top of the workspace. The symbol label, timeframe buttons, chart type
 * (candles or the volume footprint, ChartTypeButton.jsx) and Indicators
 * are real; everything else renders as `notBuilt` until its feature exists
 * (see ToolbarButton.jsx).
 *
 * Author: @DS
 */

import { Camera, Maximize, Redo2, Settings, SquareFunction, Undo2 } from "lucide-react";

import { TIMEFRAMES } from "../../lib/timeframes.js";
import ChartTypeButton from "./ChartTypeButton.jsx";
import styles from "./TopBar.module.css";
import ToolbarButton, { ToolbarDivider } from "./ToolbarButton.jsx";

export default function TopBar({ symbol, timeframe, onTimeframeChange, onIndicatorsClick, chartSettings }) {
  return (
    <header className={styles.bar}>
      <span className={styles.brand}>
        DS<span className={styles.brandAccent}>View</span>
      </span>
      <ToolbarDivider />
      <span className={styles.symbol}>{symbol}</span>
      <ToolbarDivider />
      <div className={styles.group} role="group" aria-label="Timeframe">
        {TIMEFRAMES.map(({ id }) => (
          <ToolbarButton
            key={id}
            label={id}
            title={`${id} candles`}
            pressed={id === timeframe}
            onClick={() => onTimeframeChange(id)}
          />
        ))}
      </div>
      <ToolbarDivider />
      <ChartTypeButton
        chartType={chartSettings.chartType}
        footprintRows={chartSettings.footprintRows}
        timeframe={timeframe}
        onChartTypeChange={chartSettings.setChartType}
        onFootprintRowsChange={chartSettings.setFootprintRows}
      />
      <ToolbarButton icon={SquareFunction} label="Indicators" title="Indicators" onClick={onIndicatorsClick} />
      <div className={styles.spacer} />
      <ToolbarButton icon={Undo2} title="Undo" notBuilt />
      <ToolbarButton icon={Redo2} title="Redo" notBuilt />
      <ToolbarDivider />
      <ToolbarButton icon={Camera} title="Screenshot" notBuilt />
      <ToolbarButton icon={Maximize} title="Fullscreen" notBuilt />
      <ToolbarButton icon={Settings} title="Chart settings" notBuilt />
    </header>
  );
}
