/**
 * FootprintLayer.js
 *
 * A lightweight-charts series primitive, attached to the candle series,
 * that draws the footprint of every bar FootprintManager.js hands it: per
 * bar a column of price rows, each split into the volume sold at the bid
 * (left, red) and bought at the ask (right, green), tinted by size and
 * written out when the cells are big enough to read; the row with the
 * most volume (the point of control) outlined; imbalances in bold colour;
 * a slim candle along the column's left edge; the bar's delta beneath it.
 *
 * zOrder "normal" draws it with the series, just above the candles: each
 * column paints its background first, hiding the candle it replaces, so a
 * bar without footprint data keeps its ordinary candle. Drawings ("top")
 * and the crosshair stay above it.
 *
 * Author: @DS
 */

import { barStats, formatVolume } from "./levels.js";

const COLUMN_WIDTH = 0.9; // of the bar spacing
const MAX_FONT_PX = 11;
const MIN_FONT_PX = 8;
const DELTA_FONT_PX = 10;

// "#26a69a" or "rgb(…)"/"rgba(…)" at `alpha`.
export function withAlpha(color, alpha) {
  const hex = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(color);
  if (hex) return `rgba(${parseInt(hex[1], 16)}, ${parseInt(hex[2], 16)}, ${parseInt(hex[3], 16)}, ${alpha})`;
  const rgb = /^rgba?\(([^,]+),([^,]+),([^,)]+)/.exec(color);
  if (rgb) return `rgba(${rgb[1].trim()}, ${rgb[2].trim()}, ${rgb[3].trim()}, ${alpha})`;
  return color;
}

export class FootprintLayer {
  /** `getView()` returns { columns: [{ x, bar, rows }], rowSize, spacing } or null, recomputed on every chart update. */
  constructor({ theme, getView }) {
    this._theme = theme;
    this._getView = getView;
    this._series = null;
    this._requestUpdate = null;
    this._view = null;
    this._paneView = {
      zOrder: () => "normal",
      renderer: () => ({ draw: (target) => this._draw(target) }),
    };
  }

  attached({ series, requestUpdate }) {
    this._series = series;
    this._requestUpdate = requestUpdate;
  }

  detached() {
    this._series = null;
    this._requestUpdate = null;
  }

  requestUpdate() {
    this._requestUpdate?.();
  }

  updateAllViews() {
    this._view = this._series ? this._getView() : null;
  }

  paneViews() {
    return [this._paneView];
  }

  _draw(target) {
    const view = this._view;
    if (!view || view.columns.length === 0) return;
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      for (const column of view.columns) {
        ctx.save();
        this._drawColumn(ctx, column, view, mediaSize);
        ctx.restore();
      }
    });
  }

  _drawColumn(ctx, { x, bar, rows }, { rowSize, spacing }, size) {
    const theme = this._theme;
    const y = (price) => this._series.priceToCoordinate(price);
    const top = y(rows[rows.length - 1].price + rowSize);
    const bottom = y(rows[0].price);
    if (top === null || bottom === null || bottom < 0 || top > size.height) return;

    const width = Math.max(6, Math.floor(spacing * COLUMN_WIDTH));
    const left = Math.round(x - width / 2);
    const strip = Math.max(3, Math.min(7, Math.round(width * 0.1)));
    const cellsLeft = left + strip + 2;
    const half = (left + width - cellsLeft) / 2;
    const stats = barStats(rows, rowSize);
    const up = bar.close >= bar.open;

    // Hide the candle this column replaces.
    ctx.fillStyle = theme.surface;
    ctx.fillRect(left - 1, Math.floor(top) - 1, width + 2, Math.ceil(bottom - top) + 2);

    // The candle itself, slim, along the left edge.
    const candleColor = up ? theme.up : theme.down;
    const high = y(bar.high);
    const low = y(bar.low);
    const open = y(bar.open);
    const close = y(bar.close);
    ctx.fillStyle = candleColor;
    if (high !== null && low !== null) ctx.fillRect(left + Math.floor(strip / 2), high, 1, Math.max(1, low - high));
    if (open !== null && close !== null) {
      ctx.fillRect(left, Math.min(open, close), strip, Math.max(1, Math.abs(open - close)));
    }

    const cellAlpha = (volume) => (volume === 0 ? 0.04 : 0.12 + 0.55 * (volume / (stats.maxCell || 1)));
    for (const row of rows) {
      const rowTop = Math.round(y(row.price + rowSize));
      const rowHeight = Math.round(y(row.price)) - rowTop;
      if (rowTop > size.height || rowTop + rowHeight < 0) continue;
      const cellHeight = Math.max(1, rowHeight - 1);
      ctx.fillStyle = withAlpha(theme.down, cellAlpha(row.sell));
      ctx.fillRect(cellsLeft, rowTop, half - 1, cellHeight);
      ctx.fillStyle = withAlpha(theme.up, cellAlpha(row.buy));
      ctx.fillRect(cellsLeft + half, rowTop, half - 1, cellHeight);

      const fontPx = Math.min(MAX_FONT_PX, rowHeight - 3, Math.floor(half / 2.6));
      if (fontPx >= MIN_FONT_PX) {
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        const middle = rowTop + rowHeight / 2;
        const sellImbalance = stats.sellImbalance.has(row.price);
        const buyImbalance = stats.buyImbalance.has(row.price);
        ctx.font = `${sellImbalance ? "bold " : ""}${fontPx}px ${theme.fontUi}`;
        ctx.fillStyle = sellImbalance ? theme.down : row.sell === 0 ? theme.textMuted : theme.text;
        ctx.fillText(formatVolume(row.sell), cellsLeft + half / 2, middle);
        ctx.font = `${buyImbalance ? "bold " : ""}${fontPx}px ${theme.fontUi}`;
        ctx.fillStyle = buyImbalance ? theme.up : row.buy === 0 ? theme.textMuted : theme.text;
        ctx.fillText(formatVolume(row.buy), cellsLeft + half * 1.5, middle);
      }

      if (row.price === stats.poc) {
        ctx.strokeStyle = theme.warning;
        ctx.lineWidth = 1;
        ctx.strokeRect(cellsLeft + 0.5, rowTop + 0.5, 2 * half - 2, cellHeight - 1);
      }
    }

    // The bar's delta, under the column.
    if (bottom + DELTA_FONT_PX + 4 < size.height) {
      ctx.font = `${DELTA_FONT_PX}px ${theme.fontUi}`;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = stats.delta >= 0 ? theme.up : theme.down;
      ctx.fillText(`${stats.delta > 0 ? "+" : ""}${formatVolume(stats.delta)}`, cellsLeft + half, bottom + 3);
    }
  }
}
