/**
 * BacktestMarkers.js
 *
 * The picked backtest run's trades on the chart (Phase 4b): one
 * lightweight-charts markers primitive on the candle series, created by
 * CandleChart.jsx with the chart, like IndicatorManager.js. setTrades()
 * gives it a run's trades (or none); refresh() re-sets the markers to the
 * bars the series has loaded, which CandleChart calls after the first page
 * of history and after every scroll-back page (markers.js says why: a
 * marker without its bar would land on the oldest one).
 *
 * Colours from chartTheme.js: entries in the up and down colours, exits in
 * the same two by whether the trade made money.
 *
 * Author: @DS
 */

import { createSeriesMarkers } from "lightweight-charts";

import { markersWithin, tradeMarkers } from "./markers.js";

export class BacktestMarkers {
  constructor({ candleSeries, barSeconds, theme }) {
    this._series = candleSeries;
    this._barSeconds = barSeconds;
    this._colors = { long: theme.up, short: theme.down, profit: theme.up, loss: theme.down };
    // autoScale (the default): the price scale makes room for a marker's
    // label, which would otherwise be cut off at the pane's top or bottom.
    this._plugin = createSeriesMarkers(candleSeries, []);
    this._markers = [];
    this._shown = 0;
  }

  setTrades(trades) {
    this._markers = trades ? tradeMarkers(trades, this._barSeconds, this._colors) : [];
    this.refresh();
  }

  refresh() {
    const data = this._series.data();
    if (data.length === 0 || this._markers.length === 0) {
      if (this._shown > 0) this._plugin.setMarkers([]);
      this._shown = 0;
      return;
    }
    const shown = markersWithin(this._markers, data[0].time, data[data.length - 1].time);
    this._plugin.setMarkers(shown);
    this._shown = shown.length;
  }

  destroy() {
    this._plugin.detach();
  }
}
