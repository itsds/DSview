/**
 * DrawingController.js
 *
 * All drawing interaction for one chart, kept outside React:
 *  - placing: with a tool active, each chart click adds an anchor point
 *    (subscribeClick) and the next one follows the mouse as a preview
 *    (subscribeCrosshairMove) until the drawing has all its points — then
 *    it's saved, selected, and the tool drops back to the crosshair;
 *  - selecting and dragging: with the crosshair, pressing on a drawing
 *    selects it and dragging moves it (or, on a selected drawing's handle,
 *    just that handle, within whatever limits its tool sets — see
 *    tools.js's moveHandle);
 *  - keys: Esc cancels placing / leaves the tool / deselects,
 *    Delete or Backspace removes the selected drawing.
 *
 * Dragging listens for `mousedown` in the capture phase on an ancestor of
 * the chart's own elements and stops it there when it lands on a
 * drawing, so lightweight-charts (which listens for mouse events on those
 * elements) never sees the press and doesn't pan the chart under the
 * drag. Touch dragging isn't handled.
 *
 * Rendering is DrawingLayer.js's job; this decides what it shows (saved
 * drawings, a drag in progress, the preview, the selection) and writes
 * finished changes to the drawing store.
 *
 * Author: @DS
 */

import { newId } from "../lib/ids.js";
import { isChartKey } from "../lib/keyboard.js";
import { createBarData } from "./barData.js";
import { createCoordinates } from "./coordinates.js";
import { DrawingLayer } from "./DrawingLayer.js";
import { CROSSHAIR_TOOL, TOOLS } from "./tools.js";

// A press has to move this far before it counts as a drag rather than a click.
const DRAG_THRESHOLD_PX = 3;

export class DrawingController {
  /**
   * `element` must contain the chart and share its top-left corner, so
   * mouse positions measured from it are price-pane coordinates.
   */
  constructor({ chart, series, volumeSeries, element, store, theme, barSeconds, onToolChange, onTextRequest }) {
    this._chart = chart;
    this._series = series;
    this._element = element;
    this._store = store;
    this._coords = createCoordinates(chart, series, barSeconds);
    this._onToolChange = onToolChange;
    this._onTextRequest = onTextRequest;

    this._tool = CROSSHAIR_TOOL;
    this._magnet = false;
    this._pending = null; // { type, points } being placed; its last point follows the mouse
    this._selectedId = null;
    this._drag = null;

    this._layer = new DrawingLayer({
      theme,
      coords: this._coords,
      bars: createBarData(chart, series, volumeSeries),
      getState: () => this._renderState(),
    });
    series.attachPrimitive(this._layer);

    chart.subscribeClick(this._handleClick);
    chart.subscribeCrosshairMove(this._handleCrosshairMove);
    element.addEventListener("mousedown", this._handleMouseDown, true);
    window.addEventListener("keydown", this._handleKeyDown);
    this._unsubscribe = store.subscribe(this._handleStoreChange);
  }

  setTool(tool) {
    if (tool === this._tool) return;
    this._tool = tool;
    this._pending = null;
    if (tool !== CROSSHAIR_TOOL) this._selectedId = null;
    this._layer.requestUpdate();
  }

  setMagnet(magnet) {
    this._magnet = magnet;
  }

  destroy() {
    this._stopDragging();
    this._chart.unsubscribeClick(this._handleClick);
    this._chart.unsubscribeCrosshairMove(this._handleCrosshairMove);
    this._element.removeEventListener("mousedown", this._handleMouseDown, true);
    window.removeEventListener("keydown", this._handleKeyDown);
    this._unsubscribe();
    this._series.detachPrimitive(this._layer);
  }

  _renderState() {
    let drawings = this._store.getAll();
    const drag = this._drag;
    if (drag?.moved) drawings = drawings.map((d) => (d.id === drag.id ? { ...d, points: drag.points } : d));
    return {
      drawings,
      pending: this._pending,
      selectedId: this._selectedId,
      interactive: this._tool === CROSSHAIR_TOOL,
    };
  }

  // Back to the crosshair, here and in App.jsx's state. Set here first so a
  // click that lands before React re-renders isn't handled with the old tool.
  _resetTool() {
    this._tool = CROSSHAIR_TOOL;
    this._pending = null;
    this._onToolChange(CROSSHAIR_TOOL);
  }

  _pointAt(x, y) {
    const time = this._coords.xToTime(x);
    let price = this._coords.yToPrice(y);
    if (time === null || price === null) return null;
    if (this._magnet) price = this._coords.snapPrice(time, price);
    return { time, price };
  }

  _finish(drawing) {
    const saved = { id: newId(), ...drawing };
    this._selectedId = saved.id;
    this._resetTool();
    this._store.add(saved); // redraws via the store subscription
  }

  // --- placing ---

  _handleClick = (param) => {
    if (this._tool === CROSSHAIR_TOOL || !param.point || param.paneIndex > 0) return;
    const point = this._pointAt(param.point.x, param.point.y);
    if (!point) return;

    if (this._tool === "text") {
      this._onTextRequest(param.point, (text) => {
        const trimmed = text?.trim();
        if (trimmed) this._finish({ type: "text", points: [point], text: trimmed });
        else this._resetTool();
      });
      return;
    }

    const tool = TOOLS[this._tool];
    const placed = this._pending ? [...this._pending.points.slice(0, -1), point] : [point];
    if (placed.length >= tool.anchors) {
      this._finish({ type: this._tool, points: tool.create ? tool.create(placed, this._coords) : placed });
    } else {
      this._pending = { type: this._tool, points: [...placed, point] };
      this._layer.requestUpdate();
    }
  };

  _handleCrosshairMove = (param) => {
    if (!this._pending || !param.point) return;
    const point = this._pointAt(param.point.x, param.point.y);
    if (!point) return;
    this._pending = { ...this._pending, points: [...this._pending.points.slice(0, -1), point] };
    this._layer.requestUpdate();
  };

  // --- selecting and dragging ---

  _localPoint(event) {
    const rect = this._element.getBoundingClientRect();
    return { x: event.clientX - rect.left, y: event.clientY - rect.top };
  }

  _handleMouseDown = (event) => {
    if (event.button !== 0 || this._tool !== CROSSHAIR_TOOL) return;
    if (event.target.closest?.("button, input")) return; // a legend button or the text editor, not the chart
    const { x, y } = this._localPoint(event);
    const pane = this._chart.paneSize(0);
    if (x > pane.width || y > pane.height) return; // on the price or time scale

    const hit = this._layer.findAt(x, y);
    if (!hit) {
      if (this._selectedId !== null) {
        this._selectedId = null;
        this._layer.requestUpdate();
      }
      return; // not ours: let the chart pan as usual
    }

    event.stopPropagation(); // keep lightweight-charts from panning under the drag
    event.preventDefault();
    const { type, points } = this._store.getAll().find((d) => d.id === hit.id);
    this._selectedId = hit.id;
    this._drag = { id: hit.id, type, handle: hit.handle, startX: x, startY: y, original: points, points, moved: false };
    window.addEventListener("mousemove", this._handleDragMove);
    window.addEventListener("mouseup", this._handleDragEnd);
    this._layer.requestUpdate();
  };

  _handleDragMove = (event) => {
    const drag = this._drag;
    const { x, y } = this._localPoint(event);
    const dx = x - drag.startX;
    const dy = y - drag.startY;
    if (!drag.moved && Math.hypot(dx, dy) < DRAG_THRESHOLD_PX) return;
    drag.moved = true;
    // Always offset from where the drag started, so snapping never accumulates.
    if (drag.handle === null) {
      drag.points = drag.original.map((point) => this._offset(point, dx, dy) ?? point);
    } else {
      const moved = this._offset(drag.original[drag.handle], dx, dy);
      const { moveHandle } = TOOLS[drag.type];
      if (moved) {
        drag.points = moveHandle
          ? moveHandle(drag.original, drag.handle, moved, this._coords)
          : drag.original.map((point, i) => (i === drag.handle ? moved : point));
      }
    }
    this._layer.requestUpdate();
  };

  // `point` moved by (dx, dy) pixels, or null where that can't be placed.
  _offset(point, dx, dy) {
    const px = this._coords.timeToX(point.time);
    const py = this._coords.priceToY(point.price);
    if (px === null || py === null) return null;
    const time = this._coords.xToTime(px + dx);
    const price = this._coords.yToPrice(py + dy);
    return time === null || price === null ? null : { time, price };
  }

  _handleDragEnd = () => {
    const drag = this._drag;
    this._stopDragging();
    if (drag?.moved) this._store.update(drag.id, { points: drag.points });
    else this._layer.requestUpdate();
  };

  _stopDragging() {
    window.removeEventListener("mousemove", this._handleDragMove);
    window.removeEventListener("mouseup", this._handleDragEnd);
    this._drag = null;
  }

  // --- keys and store ---

  _handleKeyDown = (event) => {
    // Typing, in a dialog, or already handled — e.g. the Esc that closes a toolbar menu.
    if (!isChartKey(event)) return;
    if (event.key === "Escape") {
      if (this._tool !== CROSSHAIR_TOOL) this._resetTool();
      else this._selectedId = null;
      this._layer.requestUpdate();
    } else if ((event.key === "Delete" || event.key === "Backspace") && this._selectedId !== null) {
      event.preventDefault();
      const id = this._selectedId;
      this._selectedId = null;
      this._store.remove(id);
    }
  };

  _handleStoreChange = () => {
    // e.g. "remove all" from the toolbar while something was selected
    if (this._selectedId !== null && !this._store.getAll().some((d) => d.id === this._selectedId)) {
      this._selectedId = null;
    }
    this._layer.requestUpdate();
  };
}
