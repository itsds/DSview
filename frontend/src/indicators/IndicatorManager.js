/**
 * IndicatorManager.js
 *
 * Draws the active indicators on one chart, outside React: owns their
 * series (and the panes the oscillators live in), recomputes them from the
 * chart's own bars, and reports their values and pane positions for the
 * legends.
 *
 * Values are computed from candleSeries.data() — what's actually on the
 * chart, including pages prepended on scroll-back and bars appended live —
 * so indicators can never drift from the candles. A full recompute is
 * cheap (O(bars) per indicator), so refresh() always recomputes everything
 * and only decides how to push it: setData() after history loads or a page
 * is prepended, update() of the last point on a live tick — the same split
 * CandleChart.jsx uses for the candles.
 *
 * Any change to the indicator list rebuilds every indicator series and pane
 * from scratch rather than diffing: there are only a handful, and it keeps
 * pane order simple (oscillators get panes 1, 2, … in list order).
 *
 * Author: @DS
 */

import { HistogramSeries, LineSeries, LineStyle } from "lightweight-charts";

import { INDICATORS, indicatorLabel, slotStyle } from "./definitions.js";

// An oscillator pane's height relative to the price pane's (stretch factor 1).
const OSCILLATOR_PANE_STRETCH = 0.3;

export class IndicatorManager {
  /**
   * `element` is the chart's container; pane positions are reported
   * relative to its top edge.
   */
  constructor({ chart, candleSeries, volumeSeries, theme, element, onValues, onLayout }) {
    this._chart = chart;
    this._candleSeries = candleSeries;
    this._volumeSeries = volumeSeries;
    this._theme = theme;
    this._element = element;
    this._onValues = onValues;
    this._onLayout = onLayout;
    this._indicators = [];
    this._built = []; // { indicator, def, paneIndex, series: { [lineKey]: series } }
    this._frame = null;

    this._resizeObserver = new ResizeObserver(this._measurePanes);
    this._resizeObserver.observe(element);
    window.addEventListener("mouseup", this._measurePanes); // pane separators are dragged with the mouse
  }

  setIndicators(indicators) {
    this._indicators = indicators;
    this._rebuild();
    this.refresh({ full: true });
    this._measurePanes();
  }

  refresh({ full = false } = {}) {
    const bars = this._candleSeries.data();
    const times = bars.map((bar) => bar.time);
    const volumeByTime = new Map(this._volumeSeries.data().map((point) => [point.time, point.value]));
    const input = {
      bars,
      closes: bars.map((bar) => bar.close),
      volumes: bars.map((bar) => volumeByTime.get(bar.time) ?? 0),
    };

    const computed = this._built.map((built) => {
      const values = bars.length === 0 ? {} : built.def.compute(input, built.indicator.params);
      for (const line of built.def.lines) {
        const series = built.series[line.key];
        const lineValues = values[line.key] ?? [];
        if (full) {
          series.setData(this._toPoints(times, lineValues, line));
        } else {
          const last = lineValues.length - 1;
          if (last >= 0 && lineValues[last] !== null) series.update(this._toPoint(times[last], lineValues[last], line));
        }
      }
      return { built, values };
    });

    this._onValues({
      indexByTime: new Map(times.map((time, i) => [time, i])),
      lastIndex: times.length - 1,
      indicators: computed.map(({ built, values }) => ({
        id: built.indicator.id,
        label: indicatorLabel(built.indicator),
        paneIndex: built.paneIndex,
        visible: built.indicator.visible,
        hasSettings: built.def.params.length > 0,
        lines: built.def.lines.map((line) => ({
          key: line.key,
          label: line.label,
          values: values[line.key] ?? [],
          ...this._lineStyle(built.indicator, line),
        })),
      })),
    });
  }

  destroy() {
    cancelAnimationFrame(this._frame);
    this._resizeObserver.disconnect();
    window.removeEventListener("mouseup", this._measurePanes);
  }

  _rebuild() {
    for (const built of this._built) Object.values(built.series).forEach((series) => this._chart.removeSeries(series));
    this._built = [];
    // Drop panes the removals left empty (lightweight-charts may already have).
    while (this._chart.panes().length > 1) this._chart.removePane(this._chart.panes().length - 1);

    let nextPane = 1;
    for (const indicator of this._indicators) {
      const def = INDICATORS[indicator.type];
      let paneIndex = 0;
      if (def.pane !== "price") {
        paneIndex = nextPane;
        nextPane += 1;
        this._chart.addPane().setStretchFactor(OSCILLATOR_PANE_STRETCH);
      }

      const series = {};
      for (const line of def.lines) {
        const definition = line.type === "histogram" ? HistogramSeries : LineSeries;
        series[line.key] = this._chart.addSeries(definition, this._seriesOptions(indicator, def, line), paneIndex);
      }

      const reference = series[def.lines.find((line) => line.type === "line").key];
      for (const level of def.levels ?? []) {
        // Thresholds (RSI's 30/70) are dashed: they mark a boundary, not a grid line.
        reference.createPriceLine({
          price: level,
          color: this._theme.textMuted,
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: false,
        });
      }
      if (def.baseline !== undefined) {
        // A zero line is an axis: a solid hairline.
        reference.createPriceLine({
          price: def.baseline,
          color: this._theme.border,
          lineWidth: 1,
          lineStyle: LineStyle.Solid,
          axisLabelVisible: false,
        });
      }

      this._built.push({ indicator, def, paneIndex, series });
    }
  }

  _seriesOptions(indicator, def, line) {
    const options = {
      visible: indicator.visible,
      priceLineVisible: false,
      lastValueVisible: line.axisLabel !== false,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    };
    if (def.range) {
      // A fixed range (RSI's 0–100) keeps its levels in place instead of the scale hugging the data.
      options.autoscaleInfoProvider = () => ({ priceRange: { minValue: def.range.min, maxValue: def.range.max } });
    }
    if (line.type === "histogram") return { ...options, color: this._theme.upVolume };
    const { color, dashed } = this._lineStyle(indicator, line);
    return { ...options, color, lineWidth: line.lineWidth ?? 2, lineStyle: dashed ? LineStyle.Dashed : LineStyle.Solid };
  }

  _lineStyle(indicator, line) {
    if (line.type === "histogram") {
      return { polarity: true, upColor: this._theme.upVolume, downColor: this._theme.downVolume, dashed: false };
    }
    return slotStyle(indicator.colorSlot + (line.colorOffset ?? 0), this._theme);
  }

  _toPoints(times, values, line) {
    const points = [];
    values.forEach((value, i) => {
      if (value !== null) points.push(this._toPoint(times[i], value, line));
    });
    return points;
  }

  _toPoint(time, value, line) {
    if (line.type !== "histogram") return { time, value };
    return { time, value, color: value >= 0 ? this._theme.upVolume : this._theme.downVolume };
  }

  // Reports each pane's top edge. Waits two frames, so any pane just added or
  // removed has been laid out by the chart before it's measured.
  _measurePanes = () => {
    cancelAnimationFrame(this._frame);
    this._frame = requestAnimationFrame(() => {
      this._frame = requestAnimationFrame(() => {
        const origin = this._element.getBoundingClientRect().top;
        this._onLayout(
          this._chart.panes().map((pane) => {
            const paneElement = pane.getHTMLElement();
            return paneElement ? paneElement.getBoundingClientRect().top - origin : null;
          }),
        );
      });
    });
  };
}
