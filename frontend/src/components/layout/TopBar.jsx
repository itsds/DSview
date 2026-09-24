/**
 * TopBar.jsx
 *
 * Symbol, timeframes, chart type, indicators, and chart actions along the
 * top of the workspace. The symbol label, timeframe buttons and Indicators
 * are real; everything else renders as `notBuilt` until its feature exists
 * (see ToolbarButton.jsx).
 *
 * Author: @DS
 */

import { Camera, ChartCandlestick, Maximize, Redo2, Settings, SquareFunction, Undo2 } from "lucide-react";

import { TIMEFRAMES } from "../../lib/timeframes.js";
import styles from "./TopBar.module.css";
import ToolbarButton, { ToolbarDivider } from "./ToolbarButton.jsx";

export default function TopBar({ symbol, timeframe, onTimeframeChange, onIndicatorsClick }) {
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
      <ToolbarButton icon={ChartCandlestick} title="Chart type" notBuilt />
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
