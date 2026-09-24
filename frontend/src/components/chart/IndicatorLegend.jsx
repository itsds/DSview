/**
 * IndicatorLegend.jsx
 *
 * One legend row per indicator — its name and settings, then each of its
 * lines' values for the bar under the crosshair — with hide / settings /
 * remove buttons that appear on hover or keyboard focus.
 *
 * Values are in text colours; a short swatch in the line's colour (and
 * dash) beside each value says which line it is, so identity never rests
 * on coloured text. PaneLegend places the same rows at the top of an
 * oscillator's own pane; Legend.jsx shows the over-the-candles ones.
 *
 * Author: @DS
 */

import { Eye, EyeOff, Settings, X } from "lucide-react";

import styles from "./IndicatorLegend.module.css";

function formatValue(value) {
  return value === null ? "—" : value.toFixed(2);
}

export function IndicatorLegendRow({ row, actions }) {
  return (
    <div className={row.visible ? styles.row : `${styles.row} ${styles.hidden}`}>
      <span className={styles.name}>{row.label}</span>
      {row.visible &&
        row.values.map((line) => (
          <span key={line.key} className={styles.value} title={line.label}>
            <span
              className={styles.swatch}
              style={{ borderTopColor: line.color, borderTopStyle: line.dashed ? "dashed" : "solid" }}
              aria-hidden="true"
            />
            {formatValue(line.value)}
          </span>
        ))}
      <span className={styles.actions}>
        <button
          type="button"
          className={styles.action}
          title={row.visible ? "Hide" : "Show"}
          aria-label={`${row.visible ? "Hide" : "Show"} ${row.label}`}
          onClick={() => actions.toggle(row.id)}
        >
          {row.visible ? <Eye size={14} aria-hidden="true" /> : <EyeOff size={14} aria-hidden="true" />}
        </button>
        {row.hasSettings && (
          <button
            type="button"
            className={styles.action}
            title="Settings"
            aria-label={`${row.label} settings`}
            onClick={() => actions.settings(row.id)}
          >
            <Settings size={14} aria-hidden="true" />
          </button>
        )}
        <button
          type="button"
          className={styles.action}
          title="Remove"
          aria-label={`Remove ${row.label}`}
          onClick={() => actions.remove(row.id)}
        >
          <X size={14} aria-hidden="true" />
        </button>
      </span>
    </div>
  );
}

export default function PaneLegend({ top, rows, actions }) {
  return (
    <div className={styles.paneLegend} style={{ top }}>
      {rows.map((row) => (
        <IndicatorLegendRow key={row.id} row={row} actions={actions} />
      ))}
    </div>
  );
}
