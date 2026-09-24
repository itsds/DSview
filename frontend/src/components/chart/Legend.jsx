/**
 * Legend.jsx
 *
 * Top-left readout over the price pane: symbol and timeframe, then
 * open/high/low/close, the bar's change, and volume — for the bar under the
 * crosshair, or the latest bar when the crosshair is off the chart — and
 * below that a row per indicator drawn over the candles (see
 * IndicatorLegend.jsx). Purely presentational; CandleChart.jsx decides
 * which bar and which values to show.
 *
 * Author: @DS
 */

import { IndicatorLegendRow } from "./IndicatorLegend.jsx";
import styles from "./Legend.module.css";

// Matches the candle series' default 2-decimal price format.
// TODO(@DS): derive from the symbol's tick size once low-priced symbols are ingested.
const PRICE_DECIMALS = 2;
const volumeFormat = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 2 });

function formatPrice(value) {
  return value.toFixed(PRICE_DECIMALS);
}

export default function Legend({ symbol, timeframe, bar, indicators, indicatorActions }) {
  return (
    <div className={styles.legend}>
      <div className={styles.main}>
        <span className={styles.title}>
          {symbol} · {timeframe}
        </span>
        {bar && <BarValues bar={bar} />}
      </div>
      {indicators.map((row) => (
        <IndicatorLegendRow key={row.id} row={row} actions={indicatorActions} />
      ))}
    </div>
  );
}

function BarValues({ bar }) {
  const change = bar.close - bar.open;
  const changePct = bar.open === 0 ? 0 : (change / bar.open) * 100;
  const sign = change >= 0 ? "+" : ""; // negatives already carry their "-"
  const tone = change >= 0 ? styles.up : styles.down;

  return (
    <>
      <Field label="O" tone={tone}>{formatPrice(bar.open)}</Field>
      <Field label="H" tone={tone}>{formatPrice(bar.high)}</Field>
      <Field label="L" tone={tone}>{formatPrice(bar.low)}</Field>
      <Field label="C" tone={tone}>{formatPrice(bar.close)}</Field>
      <span className={tone}>
        {sign}
        {formatPrice(change)} ({sign}
        {changePct.toFixed(2)}%)
      </span>
      {bar.volume !== undefined && (
        <Field label="Vol" tone={tone}>{volumeFormat.format(bar.volume)}</Field>
      )}
    </>
  );
}

function Field({ label, tone, children }) {
  return (
    <span className={styles.field}>
      <span className={styles.label}>{label}</span>
      <span className={tone}>{children}</span>
    </span>
  );
}
