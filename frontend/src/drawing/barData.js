/**
 * barData.js
 *
 * Read access to the chart's bars, for drawing tools computed from price
 * data rather than from their anchors alone — Anchored VWAP now, the
 * regression channel later. OHLC comes from the candle series and volume
 * from the volume series, both read at call time, so it always includes
 * pages loaded on scroll-back and the live bar.
 *
 * Author: @DS
 */

export function createBarData(chart, candleSeries, volumeSeries) {
  const timeScale = chart.timeScale();

  function barAt(index) {
    const bar = candleSeries.dataByIndex(index);
    if (!bar) return null;
    const volume = volumeSeries.dataByIndex(index)?.value ?? 0;
    return { index, time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close, volume };
  }

  return {
    /**
     * The bars from the one `from` falls in (the last at or before it — or
     * the first bar, if `from` is older than all of them) through the last
     * one at or before `to`, oldest first, as
     * { index, time, open, high, low, close, volume }. `index` is the bar's
     * logical index on the time scale.
     */
    between(from, to = Infinity) {
      let index = timeScale.timeToIndex(from, true); // first bar at or after `from`, else the last bar
      if (index === null) return []; // no data yet
      if (index > 0 && candleSeries.dataByIndex(index)?.time > from) index -= 1;
      const bars = [];
      for (let bar = barAt(index); bar && bar.time <= to; bar = barAt(bar.index + 1)) bars.push(bar);
      return bars;
    },
  };
}
