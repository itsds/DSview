/**
 * legend.js
 *
 * Turns IndicatorManager's computed values into legend rows for one bar —
 * the bar under the crosshair, or the latest bar when there isn't one.
 * Off the chart, a line with no value on the latest bar shows its latest
 * value instead: Binance Futures' ratios (futuresRatios.js) only arrive
 * once a period has closed, so the bar in progress never has one.
 *
 * Author: @DS
 */

export function legendRowsAt(source, time) {
  if (!source || source.indicators.length === 0) return [];
  const hovering = time !== null && source.indexByTime.has(time);
  const index = hovering ? source.indexByTime.get(time) : source.lastIndex;
  return source.indicators.map((indicator) => ({
    id: indicator.id,
    label: indicator.label,
    paneIndex: indicator.paneIndex,
    visible: indicator.visible,
    hasSettings: indicator.hasSettings,
    fromServer: indicator.fromServer,
    source: indicator.source ?? null,
    precision: indicator.precision ?? 2,
    values: indicator.lines.map((line) => {
      const value = index >= 0 ? (line.values[index] ?? (hovering ? null : latestValue(line.values, index))) : null;
      const color = line.polarity ? (value !== null && value < 0 ? line.downColor : line.upColor) : line.color;
      return { key: line.key, label: line.label, value, color, dashed: line.dashed };
    }),
  }));
}

function latestValue(values, from) {
  for (let i = Math.min(from, values.length - 1); i >= 0; i -= 1) {
    if (values[i] !== null && values[i] !== undefined) return values[i];
  }
  return null;
}
