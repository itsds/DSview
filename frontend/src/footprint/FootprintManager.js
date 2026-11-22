/**
 * FootprintManager.js
 *
 * The volume footprint on one chart, kept outside React like
 * DrawingController.js and IndicatorManager.js: CandleChart.jsx creates it
 * on a footprint timeframe (1m to 1h, Phase 3's kickoff decision 4) and
 * tells it whether the footprint is on, which row size to use, and every
 * live minute.
 *
 * On every chart update (FootprintLayer.js's updateAllViews) it works out
 * what to draw:
 *
 *  - **Zoom.** Only once bars are at least MIN_BAR_SPACING_PX apart, far
 *    enough to read cells; zoomed out, the chart stays plain candles.
 *    Turning the footprint on zooms in to FOOTPRINT_BAR_SPACING_PX if the
 *    chart is further out, so it shows at once, with a narrower right
 *    margin (the chart's own is in bars, mostly empty at this width).
 *  - **Row size.** The setting's fixed size, or on "auto" the smallest of
 *    levels.js's ROW_SIZES whose rows are MIN_ROW_PX tall at the price
 *    scale's current zoom (kickoff decision 3: stored at $1, merged for
 *    display).
 *  - **Data.** The bars on screen, from a FootprintStore.js. Any it lacks
 *    at that row size are fetched over REST in one request a little after
 *    the chart settles (scrolling and zooming fire many updates), so only
 *    the bars on screen are ever fetched. Live minutes from
 *    /ws/footprint go into the store too; the bar in progress is seeded
 *    with its earlier minutes on a bigger timeframe.
 *
 * `onStatus` hears what the legend should say: { state: "off" | "zoom" |
 * "price" | "on", rowSize }.
 *
 * Author: @DS
 */

import { MismatchDirection } from "lightweight-charts";

import { fetchFootprint } from "../api/footprint.js";
import { timeframeSeconds } from "../lib/timeframes.js";
import { FootprintLayer } from "./FootprintLayer.js";
import { createFootprintStore } from "./FootprintStore.js";
import { FOOTPRINT_PRICE_STEP, pickRowSize } from "./levels.js";

export const MIN_BAR_SPACING_PX = 48;
export const FOOTPRINT_BAR_SPACING_PX = 72;
const FOOTPRINT_RIGHT_OFFSET_BARS = 2;
const FETCH_DELAY_MS = 150;
const RETRY_AFTER_MS = 10_000;
const MAX_BARS_PER_REQUEST = 500; // backend/routers/footprint.py's MAX_BARS

export class FootprintManager {
  constructor({ chart, candleSeries, symbol, timeframe, theme, onStatus }) {
    this._chart = chart;
    this._series = candleSeries;
    this._symbol = symbol;
    this._timeframe = timeframe;
    this._barSeconds = timeframeSeconds(timeframe);
    this._onStatus = onStatus;
    this._enabled = false;
    this._rowSetting = "auto";
    this._status = null;
    this._store = createFootprintStore(this._barSeconds);
    this._pending = new Set(); // `${rowSize}:${time}` being fetched
    this._failedUntil = 0; // after a failed fetch, wait before the next
    this._seeding = new Set();
    this._wanted = null;
    this._timer = null;
    this._destroyed = false;

    this._layer = new FootprintLayer({ theme, getView: () => this._view() });
    candleSeries.attachPrimitive(this._layer);
  }

  setEnabled(enabled) {
    if (enabled === this._enabled) return;
    this._enabled = enabled;
    // Minutes that arrived while it was off went unheard: start afresh.
    const timeScale = this._chart.timeScale();
    if (!enabled) {
      this._resetData();
      timeScale.applyOptions({ rightOffset: this._rightOffset });
    } else {
      // Bars this wide leave the usual right margin (in bars) mostly empty.
      this._rightOffset ??= timeScale.options().rightOffset;
      const options = { rightOffset: FOOTPRINT_RIGHT_OFFSET_BARS };
      if (timeScale.options().barSpacing < FOOTPRINT_BAR_SPACING_PX) options.barSpacing = FOOTPRINT_BAR_SPACING_PX;
      timeScale.applyOptions(options);
    }
    this._layer.requestUpdate();
  }

  /** "auto", or a row size in dollars. */
  setRowSize(setting) {
    this._rowSetting = setting;
    this._layer.requestUpdate();
  }

  /** A live minute from /ws/footprint: { time, levels } at $1. */
  addLive({ time, levels }) {
    if (!this._enabled) return;
    const unseeded = this._store.addLiveMinute(time, levels);
    if (unseeded !== null) this._seed(unseeded);
    this._layer.requestUpdate();
  }

  destroy() {
    this._destroyed = true;
    clearTimeout(this._timer);
    this._series.detachPrimitive(this._layer);
  }

  _resetData() {
    this._store = createFootprintStore(this._barSeconds);
    this._pending.clear();
    this._seeding.clear();
    this._wanted = null;
  }

  _setStatus(status) {
    const changed = !this._status || this._status.state !== status.state || this._status.rowSize !== status.rowSize;
    if (!changed) return;
    this._status = status;
    // Not from inside the chart's own update: React state changes there re-render mid-paint.
    queueMicrotask(() => !this._destroyed && this._onStatus?.(status));
  }

  // --- what to draw, from the chart's current state ---

  _view() {
    if (!this._enabled) {
      this._setStatus({ state: "off", rowSize: null });
      return null;
    }
    const timeScale = this._chart.timeScale();
    const range = timeScale.getVisibleLogicalRange();
    const first = range && this._series.dataByIndex(Math.max(0, Math.ceil(range.from)), MismatchDirection.NearestRight);
    if (!range || !first) return null;

    const spacing = timeScale.logicalToCoordinate(1) - timeScale.logicalToCoordinate(0);
    if (!(spacing >= MIN_BAR_SPACING_PX)) {
      this._setStatus({ state: "zoom", rowSize: null });
      return null;
    }

    const y0 = this._series.priceToCoordinate(first.close);
    const y1 = this._series.priceToCoordinate(first.close + FOOTPRINT_PRICE_STEP);
    const pixelsPerDollar = y0 === null || y1 === null ? null : Math.abs(y0 - y1) / FOOTPRINT_PRICE_STEP;
    const rowSize = pickRowSize(pixelsPerDollar, this._rowSetting);
    if (rowSize === null) {
      this._setStatus({ state: "price", rowSize: null });
      return null;
    }
    this._setStatus({ state: "on", rowSize });

    const columns = [];
    const times = [];
    const last = Math.floor(range.to);
    for (let index = Math.max(0, Math.floor(range.from)); index <= last; index += 1) {
      const bar = this._series.dataByIndex(index);
      if (!bar) break; // past the newest bar
      times.push(bar.time);
      const rows = this._store.bar(bar.time, rowSize);
      if (rows && rows.length > 0) columns.push({ x: timeScale.logicalToCoordinate(index), bar, rows });
    }
    this._want(rowSize, times);
    return { columns, rowSize, spacing };
  }

  // --- fetching ---

  _missing(rowSize, times) {
    return this._store.missing(times, rowSize).filter((time) => !this._pending.has(`${rowSize}:${time}`));
  }

  // Throttled: scrolling and zooming redraw many times a second.
  _want(rowSize, times) {
    if (this._missing(rowSize, times).length === 0) return;
    this._wanted = { rowSize, times };
    if (this._timer === null) this._timer = setTimeout(() => this._fetchWanted(), FETCH_DELAY_MS);
  }

  _fetchWanted() {
    this._timer = null;
    if (this._destroyed || !this._wanted) return;
    if (Date.now() < this._failedUntil) {
      this._timer = setTimeout(() => this._fetchWanted(), this._failedUntil - Date.now());
      return;
    }
    const { rowSize, times } = this._wanted;
    const missing = this._missing(rowSize, times);
    if (missing.length === 0) return;

    const start = Math.min(...missing);
    const end = Math.min(Math.max(...missing) + this._barSeconds, start + MAX_BARS_PER_REQUEST * this._barSeconds);
    const requested = missing.filter((time) => time < end).map((time) => `${rowSize}:${time}`);
    requested.forEach((key) => this._pending.add(key));
    const store = this._store;
    fetchFootprint(this._symbol, { timeframe: this._timeframe, start, end, rowSize })
      .then((bars) => {
        if (this._destroyed || store !== this._store) return;
        store.addHistory(rowSize, start, end, bars);
        this._layer.requestUpdate();
      })
      .catch((error) => {
        console.error("Loading the footprint failed:", error);
        this._failedUntil = Date.now() + RETRY_AFTER_MS;
      })
      .finally(() => {
        requested.forEach((key) => this._pending.delete(key));
        // Anything still missing (scrolled meanwhile, or a capped request) on the next round.
        if (!this._destroyed) this._layer.requestUpdate();
      });
  }

  _seed(bar) {
    if (this._seeding.has(bar)) return;
    this._seeding.add(bar);
    const store = this._store;
    fetchFootprint(this._symbol, { timeframe: "1m", start: bar, end: bar + this._barSeconds, rowSize: FOOTPRINT_PRICE_STEP })
      .then((minutes) => {
        if (this._destroyed || store !== this._store) return;
        store.seed(bar, minutes);
        this._layer.requestUpdate();
      })
      .catch((error) => {
        console.error("Seeding the live footprint bar failed:", error);
        setTimeout(() => this._seeding.delete(bar), RETRY_AFTER_MS); // the next live minute tries again
      });
  }
}
