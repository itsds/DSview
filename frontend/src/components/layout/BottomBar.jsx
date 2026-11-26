/**
 * BottomBar.jsx
 *
 * The Strategy Tester's toggle (Phase 4b: the backtest results panel opens
 * under the chart, components/backtest/StrategyTester.jsx), range presets
 * and price-scale toggles along the bottom of the chart (not built yet),
 * plus a UTC clock. UTC because lightweight-charts plots
 * every timestamp in UTC — the clock matches the chart's time axis until
 * timezone selection exists.
 *
 * Author: @DS
 */

import { useEffect, useState } from "react";

import styles from "./BottomBar.module.css";
import { FlaskConical } from "lucide-react";

import ToolbarButton, { ToolbarDivider } from "./ToolbarButton.jsx";

const RANGES = ["1D", "5D", "1M"];

function useNow() {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  return now;
}

export default function BottomBar({ testerOpen, onTesterToggle }) {
  const now = useNow();

  return (
    <footer className={styles.bar}>
      <ToolbarButton
        icon={FlaskConical}
        label="Strategy Tester"
        title="Backtest results"
        pressed={testerOpen}
        onClick={onTesterToggle}
      />
      <ToolbarDivider />
      {RANGES.map((range) => (
        <ToolbarButton key={range} label={range} title={`Zoom to ${range}`} notBuilt />
      ))}
      <div className={styles.spacer} />
      <time className={styles.clock} dateTime={now.toISOString()}>
        {now.toISOString().slice(11, 19)} UTC
      </time>
      <ToolbarDivider />
      <ToolbarButton label="%" title="Percent price scale" pressed={false} notBuilt />
      <ToolbarButton label="log" title="Logarithmic price scale" pressed={false} notBuilt />
      <ToolbarButton label="auto" title="Auto-fit price scale" pressed={false} notBuilt />
    </footer>
  );
}
