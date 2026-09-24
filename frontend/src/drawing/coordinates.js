/**
 * coordinates.js
 *
 * Converts between a drawing's stored time/price and pixels on the price
 * pane.
 *
 * Price is direct (series.priceToCoordinate / coordinateToPrice). Time is
 * not: timeToCoordinate() only knows times that are exactly some bar's
 * start, so a drawing anchored on a 1m bar would vanish on the 5m chart,
 * where 10:03 isn't a bar. Instead a time is placed relative to the bar
 * timeToIndex(time, true) finds for it — the first bar at or after it, or
 * the last bar for any later time:
 *
 *     logical = index + (time − barTime) / barSeconds
 *
 * which lands it inside the right bar on any timeframe, and in the empty
 * space right of the last bar for future times. The other way round, x
 * snaps to the bar under it (coordinateToLogical already returns that
 * bar's index), extrapolating one bar per barSeconds past either end.
 *
 * Author: @DS
 */

import { MismatchDirection } from "lightweight-charts";

export function createCoordinates(chart, series, barSeconds) {
  const timeScale = chart.timeScale();

  function timeToLogical(time) {
    const index = timeScale.timeToIndex(time, true);
    if (index === null) return null; // no data yet
    const bar = series.dataByIndex(index);
    return bar ? index + (time - bar.time) / barSeconds : index;
  }

  function xToTime(x) {
    const index = timeScale.coordinateToLogical(x);
    if (index === null) return null;
    const bar = series.dataByIndex(index);
    if (bar) return bar.time;
    // Past either end of the data: extrapolate from the nearest bar.
    const edge = series.dataByIndex(index, index < 0 ? MismatchDirection.NearestRight : MismatchDirection.NearestLeft);
    if (!edge) return null;
    return edge.time + (index - timeScale.timeToIndex(edge.time)) * barSeconds;
  }

  return {
    barSeconds,
    timeToLogical,
    timeToX(time) {
      const logical = timeToLogical(time);
      return logical === null ? null : timeScale.logicalToCoordinate(logical);
    },
    // A bar's logical index, as barData.js reports it, to x.
    indexToX: (index) => timeScale.logicalToCoordinate(index),
    xToTime,
    priceToY: (price) => series.priceToCoordinate(price),
    yToPrice: (y) => series.coordinateToPrice(y),
    formatPrice: (price) => series.priceFormatter().format(price),
    // Magnet mode: whichever of the bar's open/high/low/close is nearest `price`.
    snapPrice(time, price) {
      const index = timeScale.timeToIndex(time);
      const bar = index === null ? null : series.dataByIndex(index);
      if (!bar) return price; // past the end of the data: nothing to snap to
      return [bar.open, bar.high, bar.low, bar.close].reduce((best, p) =>
        Math.abs(p - price) < Math.abs(best - price) ? p : best,
      );
    },
  };
}
