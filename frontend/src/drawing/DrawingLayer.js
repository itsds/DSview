/**
 * DrawingLayer.js
 *
 * A lightweight-charts series primitive that renders every drawing — plus
 * the one being placed and handles on the selected one — on the price
 * pane. One primitive for all drawings rather than one per drawing, so
 * adding or removing a drawing is a list change, not an attach/detach.
 *
 * Pixel positions are recomputed in updateAllViews(), which the chart
 * calls whenever it scrolls, zooms, resizes or gets new data, and reused
 * by both drawing and hit-testing — so what you click is exactly what's
 * drawn.
 *
 * Author: @DS
 */

import { HANDLE_RADIUS_PX, TOOLS } from "./tools.js";

// Handles are easier to grab than they look.
const HANDLE_HIT_PX = HANDLE_RADIUS_PX + 3;

function formatTime(time) {
  return new Date(time * 1000).toISOString().slice(0, 16).replace("T", " "); // UTC, like the chart's time axis
}

export class DrawingLayer {
  constructor({ theme, coords, bars, getState }) {
    this._theme = theme;
    this._coords = coords;
    this._bars = bars;
    this._getState = getState;
    this._chart = null;
    this._requestUpdate = null;
    this._items = []; // { drawing, pts, selected, handles } for everything drawable right now
    this._size = { width: 0, height: 0 };
    this._interactive = false;
    this._paneView = {
      zOrder: () => "top",
      renderer: () => ({ draw: (target) => this._draw(target) }),
    };
  }

  // --- lightweight-charts series primitive ---

  attached({ chart, requestUpdate }) {
    this._chart = chart;
    this._requestUpdate = requestUpdate;
  }

  detached() {
    this._chart = null;
    this._requestUpdate = null;
  }

  updateAllViews() {
    if (!this._chart) return;
    const { drawings, pending, selectedId, interactive } = this._getState();
    this._size = this._chart.paneSize(0);
    this._interactive = interactive;
    const all = pending ? [...drawings, { id: null, ...pending }] : drawings;
    this._items = [];
    for (const drawing of all) {
      const tool = TOOLS[drawing.type];
      if (!tool) continue; // saved by a version of the app with a tool this one lacks
      const pts = drawing.points.map((p) => ({ x: this._coords.timeToX(p.time), y: this._coords.priceToY(p.price) }));
      if (pts.some((p) => p.x === null || p.y === null)) continue; // no data to place it against yet
      const item = { drawing, pts, selected: drawing.id !== null && drawing.id === selectedId, handles: [] };
      // Only the selected drawing shows handles, so only its are worked out.
      if (item.selected) item.handles = tool.handles ? tool.handles(this._context(item)) : pts;
      this._items.push(item);
    }
  }

  paneViews() {
    return [this._paneView];
  }

  priceAxisViews() {
    return this._items.flatMap((item) => {
      const label = TOOLS[item.drawing.type].priceAxisLabel;
      if (!label) return [];
      if (label === true) return [this._axisLabel(item.pts[0].y, this._coords.formatPrice(item.drawing.points[0].price))];
      const price = label(this._context(item));
      const y = price === null ? null : this._coords.priceToY(price);
      return y === null ? [] : [this._axisLabel(y, this._coords.formatPrice(price))];
    });
  }

  timeAxisViews() {
    return this._items
      .filter((item) => TOOLS[item.drawing.type].timeAxisLabel)
      .map((item) => this._axisLabel(item.pts[0].x, formatTime(item.drawing.points[0].time)));
  }

  // Only sets the cursor over drawings; selecting and dragging are DrawingController's.
  hitTest(x, y) {
    if (!this._interactive) return null;
    const hit = this.findAt(x, y);
    if (!hit) return null;
    return { externalId: hit.id, zOrder: "top", cursorStyle: hit.handle === null ? "pointer" : "move" };
  }

  // --- for DrawingController ---

  requestUpdate() {
    this._requestUpdate?.();
  }

  /** The saved drawing at (x, y); `handle` is a handle index when on one of the selected drawing's handles. */
  findAt(x, y) {
    const selected = this._items.find((item) => item.selected);
    const handle = selected?.handles.findIndex((p) => Math.hypot(p.x - x, p.y - y) <= HANDLE_HIT_PX) ?? -1;
    if (handle !== -1) return { id: selected.drawing.id, handle };
    // Topmost first: later drawings are drawn over earlier ones.
    for (let i = this._items.length - 1; i >= 0; i -= 1) {
      const item = this._items[i];
      if (item.drawing.id !== null && TOOLS[item.drawing.type].hit(this._context(item), x, y)) {
        return { id: item.drawing.id, handle: null };
      }
    }
    return null;
  }

  // --- internals ---

  _context(item, size = this._size) {
    return { ...item, size, theme: this._theme, coords: this._coords, bars: this._bars };
  }

  _axisLabel(coordinate, text) {
    const theme = this._theme;
    return {
      coordinate: () => coordinate,
      text: () => text,
      textColor: () => theme.onAccent,
      backColor: () => theme.accent,
      visible: () => true,
    };
  }

  _draw(target) {
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      for (const item of this._items) {
        ctx.save();
        TOOLS[item.drawing.type].draw(ctx, this._context(item, mediaSize));
        ctx.restore();
        if (item.selected) this._drawHandles(ctx, item.handles);
      }
    });
  }

  _drawHandles(ctx, pts) {
    ctx.save();
    ctx.fillStyle = this._theme.surface;
    ctx.strokeStyle = this._theme.accent;
    ctx.lineWidth = 1.5;
    for (const p of pts) {
      ctx.beginPath();
      ctx.arc(p.x, p.y, HANDLE_RADIUS_PX, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
    }
    ctx.restore();
  }
}
