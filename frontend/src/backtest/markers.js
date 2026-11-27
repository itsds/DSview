/**
 * markers.js
 *
 * A backtest run's trades as lightweight-charts series markers (Phase 4b),
 * on every timeframe (kickoff decision 3): each fill on the bar of the
 * chart's timeframe that contains it, whatever timeframe the run traded,
 * so on bigger timeframes several can share a bar.
 *
 * - An entry: an arrow up under the bar for a long, down over it for a
 *   short, with the trade's number.
 * - An exit: a dot on the other side, with the trade's net return ("end"
 *   when the data ran out with the trade still open).
 *
 * Markers must be in time order: by bar, then by when each fill happened,
 * and at the same moment an exit before an entry, so a reversal reads
 * "exit, then the new side". The one exit that comes after an entry at
 * the same moment is the data's end: a trade closed at the last bar's
 * close, after that bar's open.
 *
 * lightweight-charts moves a marker it has no bar for to the nearest bar it
 * has: every trade before the oldest loaded bar would pile up on it. So
 * `markersWithin()` keeps only those inside what the chart has loaded;
 * BacktestMarkers.js asks again whenever scroll-back loads more.
 *
 * Author: @DS
 */

import { bucketStart } from "../lib/timeframes.js";

export function formatReturn(pct) {
  const sign = pct > 0 ? "+" : pct < 0 ? "−" : "";
  return `${sign}${Math.abs(pct).toFixed(2)}%`;
}

/** Every trade's entry and exit marker on `barSeconds` bars, in time order. */
export function tradeMarkers(trades, barSeconds, colors) {
  const markers = [];
  for (const trade of trades) {
    const long = trade.side === "long";
    const result = formatReturn(trade.returnPct);
    markers.push({
      time: bucketStart(trade.entryTime, barSeconds),
      at: trade.entryTime,
      order: 1,
      position: long ? "belowBar" : "aboveBar",
      shape: long ? "arrowUp" : "arrowDown",
      color: long ? colors.long : colors.short,
      text: `${long ? "Long" : "Short"} #${trade.number}`,
      id: `entry-${trade.number}`,
    });
    markers.push({
      time: bucketStart(trade.exitTime, barSeconds),
      at: trade.exitTime,
      order: trade.closedByEnd ? 2 : 0,
      position: long ? "aboveBar" : "belowBar",
      shape: "circle",
      color: trade.pnl >= 0 ? colors.profit : colors.loss,
      text: trade.closedByEnd ? `end ${result}` : result,
      id: `exit-${trade.number}`,
    });
  }
  markers.sort((a, b) => a.time - b.time || a.at - b.at || a.order - b.order);
  return markers.map(({ at, order, ...marker }) => marker);
}

/** The markers on bars from `first` to `last` (Unix seconds): what the chart has loaded. */
export function markersWithin(markers, first, last) {
  return markers.filter((marker) => marker.time >= first && marker.time <= last);
}
