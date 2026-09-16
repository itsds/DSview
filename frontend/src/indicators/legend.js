/**
 * legend.js
 *
 * Turns IndicatorManager's computed values into legend rows for one bar —
 * the bar under the crosshair, or the latest bar when there isn't one.
 *
 * Author: @DS
 */

export function legendRowsAt(source, time) {
  if (!source || source.indicators.length === 0) return [];
  const index = time !== null && source.indexByTime.has(time) ? source.indexByTime.get(time) : source.lastIndex;
  return source.indicators.map((indicator) => ({
    id: indicator.id,
    label: indicator.label,
    paneIndex: indicator.paneIndex,
    visible: indicator.visible,
    hasSettings: indicator.hasSettings,
    values: indicator.lines.map((line) => {
      const value = index >= 0 ? (line.values[index] ?? null) : null;
      const color = line.polarity ? (value !== null && value < 0 ? line.downColor : line.upColor) : line.color;
      return { key: line.key, label: line.label, value, color, dashed: line.dashed };
    }),
  }));
}
