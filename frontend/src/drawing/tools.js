/**
 * tools.js
 *
 * One entry per drawing tool: how many anchor points it takes, how to
 * draw it, and how to hit-test it. Everything here works in pixels on the
 * price pane — DrawingLayer.js converts a drawing's stored time/price
 * points to pixels (via coordinates.js) and passes them in as `pts`, so
 * tools never touch the chart API directly.
 *
 * Every draw() and hit() receives the same context:
 *   { drawing, pts, selected, size, theme, coords, bars }
 * where `bars` (barData.js) reads the chart's candles, for tools drawn from
 * price data rather than from their anchors alone.
 *
 * Besides `anchors`, draw() and hit(), a tool can define:
 *   - create(points, coords): the points to store, given the clicks that
 *     placed it — for a tool that sizes itself from a single click;
 *   - moveHandle(points, handle, point, coords): every point once handle
 *     `handle` is dragged to `point` — for a handle that only moves one
 *     way, or takes other points with it. Default: just that point moves;
 *   - handles(context): where the selected drawing's handles are drawn and
 *     grabbed, in pixels. Default: at its points;
 *   - priceAxisLabel / timeAxisLabel: `true` labels the first point on that
 *     axis. priceAxisLabel can instead be a function of the context that
 *     returns the price to label, or null for none.
 *
 * Rays and extended lines store only their two anchors and are run on to
 * the edge of the pane afresh on every draw, so they stay edge to edge
 * through any scroll or zoom.
 *
 * Author: @DS
 */

import { anchoredVwap } from "../indicators/math.js";

// The "no tool" pointer: selects and drags drawings instead of placing them.
export const CROSSHAIR_TOOL = "crosshair";

export const HIT_TOLERANCE_PX = 6;
export const HANDLE_RADIUS_PX = 4.5;

const LINE_WIDTH = 2;
const TEXT_SIZE_PX = 14;
const LABEL_SIZE_PX = 12;
const LABEL_GAP_PX = 6; // between a shape and its label
const ARROW_HEAD_PX = 7;
const ARROW_HEAD_ANGLE = Math.PI / 7;
const ANGLE_ARC_RADIUS_PX = 40;
// How far past the pane edge a ray is drawn, so its line cap never shows.
const EDGE_OVERSHOOT_PX = 10;

// Retracement levels: 0 at the second (end) point, 1 at the first (start)
// point — so drawing from a swing low up to a swing high puts 0 at the high
// and measures the pullback down from there.
const FIB_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1];
const FIB_LEVEL_COLORS = ["textMuted", "down", "warning", "up", "info", "accent", "textMuted"]; // theme keys

function font(sizePx, theme) {
  return `${sizePx}px ${theme.fontUi}`;
}

// Distance from (x, y) to the line through a and b, over the stretch where
// t (0 at a, 1 at b) lies in [tMin, tMax]: [0, 1] for a segment, [0, ∞)
// for a ray, (−∞, ∞) for an extended line.
function distanceToLine(x, y, a, b, tMin = 0, tMax = 1) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lengthSq = dx * dx + dy * dy;
  const t = lengthSq === 0 ? 0 : Math.max(tMin, Math.min(tMax, ((x - a.x) * dx + (y - a.y) * dy) / lengthSq));
  return Math.hypot(x - (a.x + t * dx), y - (a.y + t * dy));
}

// Where the line from `from` through `through` leaves the pane — but never
// short of `through` itself.
function edgePoint(from, through, size) {
  const dx = through.x - from.x;
  const dy = through.y - from.y;
  if (dx === 0 && dy === 0) return through; // both anchors on one pixel: no direction to run in
  const exits = [];
  if (dx !== 0) exits.push(((dx > 0 ? size.width + EDGE_OVERSHOOT_PX : -EDGE_OVERSHOOT_PX) - from.x) / dx);
  if (dy !== 0) exits.push(((dy > 0 ? size.height + EDGE_OVERSHOOT_PX : -EDGE_OVERSHOOT_PX) - from.y) / dy);
  const t = Math.max(1, Math.min(...exits));
  return { x: from.x + t * dx, y: from.y + t * dy };
}

function strokeLine(ctx, a, b, color, { width = LINE_WIDTH, dash = [] } = {}) {
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.setLineDash(dash);
  ctx.beginPath();
  ctx.moveTo(a.x, a.y);
  ctx.lineTo(b.x, b.y);
  ctx.stroke();
  ctx.setLineDash([]);
}

function drawArrow(ctx, from, to, color) {
  strokeLine(ctx, from, to, color, { width: 1 });
  if (Math.hypot(to.x - from.x, to.y - from.y) < ARROW_HEAD_PX) return; // too short for a head
  const angle = Math.atan2(to.y - from.y, to.x - from.x);
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(to.x, to.y);
  for (const side of [-1, 1]) {
    const edge = angle + side * ARROW_HEAD_ANGLE;
    ctx.lineTo(to.x - ARROW_HEAD_PX * Math.cos(edge), to.y - ARROW_HEAD_PX * Math.sin(edge));
  }
  ctx.closePath();
  ctx.fill();
}

/**
 * Lines of text in a filled box. `x` is the box's centre, left or right
 * edge (per `align`), and `y` its top, bottom or middle (per `baseline`).
 */
function drawLabel(ctx, theme, lines, { x, y, align = "center", baseline = "top", background, color, border = null }) {
  ctx.font = font(LABEL_SIZE_PX, theme);
  const lineHeight = LABEL_SIZE_PX + 4;
  const width = Math.max(...lines.map((line) => ctx.measureText(line).width)) + 16;
  const height = lines.length * lineHeight + 8;
  const left = { center: x - width / 2, left: x, right: x - width }[align];
  const top = { top: y, bottom: y - height, middle: y - height / 2 }[baseline];
  ctx.fillStyle = background;
  ctx.fillRect(left, top, width, height);
  if (border) {
    ctx.strokeStyle = border;
    ctx.lineWidth = 1;
    ctx.strokeRect(left + 0.5, top + 0.5, width - 1, height - 1);
  }
  ctx.fillStyle = color;
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  lines.forEach((line, i) => ctx.fillText(line, left + width / 2, top + 5 + i * lineHeight));
}

function bounds(a, b) {
  return {
    left: Math.min(a.x, b.x),
    right: Math.max(a.x, b.x),
    top: Math.min(a.y, b.y),
    bottom: Math.max(a.y, b.y),
  };
}

function contains(rect, x, y, pad = 0) {
  return x >= rect.left - pad && x <= rect.right + pad && y >= rect.top - pad && y <= rect.bottom + pad;
}

// Shared hit-tests: the line between the two anchors, or the box they span.
function hitSegment({ pts }, x, y) {
  return distanceToLine(x, y, pts[0], pts[1]) <= HIT_TOLERANCE_PX;
}

function hitBox({ pts }, x, y) {
  return contains(bounds(pts[0], pts[1]), x, y, HIT_TOLERANCE_PX);
}

let measuringContext = null;

// Text width outside a draw() call (for hit-testing), from a throwaway canvas.
function measureText(text, cssFont) {
  measuringContext ??= document.createElement("canvas").getContext("2d");
  measuringContext.font = cssFont;
  return measuringContext.measureText(text).width;
}

function textBounds({ drawing, pts, theme }) {
  const width = measureText(drawing.text, font(TEXT_SIZE_PX, theme));
  return { left: pts[0].x, right: pts[0].x + width, top: pts[0].y, bottom: pts[0].y + TEXT_SIZE_PX * 1.3 };
}

function fibLevels({ drawing, coords }) {
  const [start, end] = drawing.points;
  return FIB_LEVELS.map((level, i) => {
    const price = end.price + (start.price - end.price) * level;
    return { level, price, y: coords.priceToY(price), colorKey: FIB_LEVEL_COLORS[i] };
  }).filter((level) => level.y !== null);
}

function formatDuration(totalSeconds) {
  const minutes = Math.round(totalSeconds / 60);
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const mins = minutes % 60;
  return [days && `${days}d`, hours && `${hours}h`, (mins || (!days && !hours)) && `${mins}m`]
    .filter(Boolean)
    .join(" ");
}

// "+12.50 (+0.02%)", from the first point's price to the second's.
function priceChange({ drawing, coords }) {
  const [a, b] = drawing.points;
  const change = b.price - a.price;
  const percent = a.price === 0 ? 0 : (change / a.price) * 100;
  const sign = change >= 0 ? "+" : "";
  return { up: change >= 0, text: `${sign}${coords.formatPrice(change)} (${sign}${percent.toFixed(2)}%)` };
}

// "12 bars, 12m" between the two points.
function timeSpan({ drawing, coords }) {
  const [a, b] = drawing.points;
  const from = coords.timeToLogical(a.time);
  const to = coords.timeToLogical(b.time);
  const bars = from === null || to === null ? null : Math.round(Math.abs(to - from));
  return [bars !== null && `${bars} bars`, formatDuration(Math.abs(b.time - a.time))].filter(Boolean).join(", ");
}

// The line's angle as drawn on screen, in degrees anticlockwise from
// pointing right. On-screen, like TradingView's, so it changes with zoom.
function screenAngle(a, b) {
  return (Math.atan2(a.y - b.y, b.x - a.x) * 180) / Math.PI;
}

function formatAngle(degrees) {
  return `${degrees.toFixed(2)}°`;
}

// --- Long and short positions ---
//
// An entry, with a profit target on one side of it and a stop loss on the
// other, held over a span of bars. Stored as four points kept in step,
//   [entry, target, stop, end] — target and stop at the entry's time, the
//   end at the entry's price —
// so the handles sit where TradingView's do (entry, target and stop down
// the left edge, the end on the right), and dragging the whole drawing,
// which shifts every point alike, keeps them in step for free.

// Placed with twice as much reward as risk, the same size on screen at any zoom.
const POSITION_TARGET_PX = 80;
const POSITION_STOP_PX = 40;
const POSITION_WIDTH_PX = 120;

function positionBox([entry, target, stop, end]) {
  return {
    left: Math.min(entry.x, end.x),
    right: Math.max(entry.x, end.x),
    top: Math.min(target.y, stop.y),
    bottom: Math.max(target.y, stop.y),
  };
}

// "Target: 77850.00 (+925.00, +1.20%)", measured from the entry.
function levelText(name, price, entryPrice, coords) {
  const change = price - entryPrice;
  const percent = entryPrice === 0 ? 0 : (change / entryPrice) * 100;
  const sign = change >= 0 ? "+" : "";
  return `${name}: ${coords.formatPrice(price)} (${sign}${coords.formatPrice(change)}, ${sign}${percent.toFixed(2)}%)`;
}

// `direction` is +1 for a long (target above the entry), −1 for a short.
function positionTool(direction) {
  return {
    anchors: 1,

    // The click was just converted from these same pixels, so there's data
    // under it and none of these conversions comes back null.
    create([entry], coords) {
      const x = coords.timeToX(entry.time);
      const y = coords.priceToY(entry.price);
      const end = coords.xToTime(x + POSITION_WIDTH_PX);
      return [
        entry,
        { time: entry.time, price: coords.yToPrice(y - direction * POSITION_TARGET_PX) },
        { time: entry.time, price: coords.yToPrice(y + direction * POSITION_STOP_PX) },
        { time: Math.max(end, entry.time + coords.barSeconds), price: entry.price },
      ];
    },

    // Target and stop move up and down only, and stay on their own side of
    // the entry; the end moves sideways only, and stays right of the entry;
    // the entry moves anywhere inside the box and takes the others with it.
    moveHandle([entry, target, stop, end], handle, { time, price }, coords) {
      const profitSide = (p) => (direction > 0 ? Math.max(p, entry.price) : Math.min(p, entry.price));
      const lossSide = (p) => (direction > 0 ? Math.min(p, entry.price) : Math.max(p, entry.price));
      if (handle === 1) return [entry, { ...target, price: profitSide(price) }, stop, end];
      if (handle === 2) return [entry, target, { ...stop, price: lossSide(price) }, end];
      if (handle === 3) return [entry, target, stop, { ...end, time: Math.max(time, entry.time + coords.barSeconds) }];
      const low = Math.min(target.price, stop.price);
      const high = Math.max(target.price, stop.price);
      const moved = { time: Math.min(time, end.time - coords.barSeconds), price: Math.min(high, Math.max(low, price)) };
      return [
        moved,
        { time: moved.time, price: target.price },
        { time: moved.time, price: stop.price },
        { time: end.time, price: moved.price },
      ];
    },

    draw(ctx, context) {
      const { drawing, pts, theme, coords } = context;
      const [entry, target, stop] = pts;
      const box = positionBox(pts);
      const width = box.right - box.left;
      const middle = (box.left + box.right) / 2;
      ctx.fillStyle = theme.upSoft;
      ctx.fillRect(box.left, Math.min(entry.y, target.y), width, Math.abs(target.y - entry.y));
      ctx.fillStyle = theme.downSoft;
      ctx.fillRect(box.left, Math.min(entry.y, stop.y), width, Math.abs(stop.y - entry.y));
      strokeLine(ctx, { x: box.left, y: entry.y }, { x: box.right, y: entry.y }, theme.textMuted, { width: 1 });

      // Each level's label just outside its zone; the ratio on the entry line.
      const [entryPoint, targetPoint, stopPoint] = drawing.points;
      const above = (y) => ({ y: y - LABEL_GAP_PX, baseline: "bottom" });
      const below = (y) => ({ y: y + LABEL_GAP_PX, baseline: "top" });
      drawLabel(ctx, theme, [levelText("Target", targetPoint.price, entryPoint.price, coords)], {
        x: middle,
        ...(direction > 0 ? above(target.y) : below(target.y)),
        background: theme.up,
        color: theme.onAccent,
      });
      drawLabel(ctx, theme, [levelText("Stop", stopPoint.price, entryPoint.price, coords)], {
        x: middle,
        ...(direction > 0 ? below(stop.y) : above(stop.y)),
        background: theme.down,
        color: theme.onAccent,
      });
      const reward = Math.abs(targetPoint.price - entryPoint.price);
      const risk = Math.abs(entryPoint.price - stopPoint.price);
      drawLabel(ctx, theme, [`Risk/reward: ${risk === 0 ? "—" : (reward / risk).toFixed(2)}`], {
        x: middle,
        y: entry.y,
        baseline: "middle",
        background: theme.surfaceHover,
        color: theme.text,
      });
    },

    hit({ pts }, x, y) {
      return contains(positionBox(pts), x, y, HIT_TOLERANCE_PX);
    },
  };
}

// --- Anchored VWAP ---
//
// The volume-weighted average of the typical price, summed from the
// anchor's bar through the latest one, so it runs on as live bars arrive.
// The anchor's time picks the bar — on any timeframe, the bar it falls in;
// its price isn't used.

// The line in pixels, each point with its value. Empty while the anchor's
// own bar isn't loaded (history pages in on scroll-back, and a future
// anchor's bar doesn't exist yet): summed from any other bar, the average
// would be wrong.
function anchoredVwapLine({ drawing, bars, coords }) {
  const anchor = drawing.points[0].time;
  const range = bars.between(anchor);
  const [first] = range;
  if (!first || first.time > anchor || first.time + coords.barSeconds <= anchor) return [];
  const values = anchoredVwap(range, range.map((bar) => bar.volume));
  return range
    .map((bar, i) => ({ x: coords.indexToX(bar.index), y: coords.priceToY(values[i]), value: values[i] }))
    .filter((point) => point.x !== null && point.y !== null);
}

// Anchored on the latest bar, the line is one point so far: stretch it into
// a short stub, so there's something to see and to grab.
function strokablePath(line) {
  if (line.length !== 1) return line;
  const [point] = line;
  return [
    { ...point, x: point.x - 4 },
    { ...point, x: point.x + 4 },
  ];
}

export const TOOLS = {
  "trend-line": {
    anchors: 2,
    draw(ctx, { pts, theme }) {
      strokeLine(ctx, pts[0], pts[1], theme.accent);
    },
    hit: hitSegment,
  },

  ray: {
    anchors: 2,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, pts[0], edgePoint(pts[0], pts[1], size), theme.accent);
    },
    hit({ pts }, x, y) {
      return distanceToLine(x, y, pts[0], pts[1], 0, Infinity) <= HIT_TOLERANCE_PX;
    },
  },

  "info-line": {
    anchors: 2,
    draw(ctx, context) {
      const { pts, theme } = context;
      const [a, b] = pts;
      strokeLine(ctx, a, b, theme.accent);
      // Beyond the end point, on the side away from the line, so it never covers it.
      const rightward = b.x >= a.x;
      drawLabel(ctx, theme, [priceChange(context).text, timeSpan(context), `∠ ${formatAngle(screenAngle(a, b))}`], {
        x: b.x + (rightward ? 2 : -2) * LABEL_GAP_PX,
        y: b.y,
        align: rightward ? "left" : "right",
        baseline: "middle",
        background: theme.surfaceHover,
        color: theme.text,
        border: theme.accent,
      });
    },
    hit: hitSegment,
  },

  "extended-line": {
    anchors: 2,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, edgePoint(pts[1], pts[0], size), edgePoint(pts[0], pts[1], size), theme.accent);
    },
    hit({ pts }, x, y) {
      return distanceToLine(x, y, pts[0], pts[1], -Infinity, Infinity) <= HIT_TOLERANCE_PX;
    },
  },

  "trend-angle": {
    anchors: 2,
    draw(ctx, { pts, theme }) {
      const [a, b] = pts;
      strokeLine(ctx, a, b, theme.accent);
      // The horizontal the angle is measured from, and an arc from it to the line.
      const radius = Math.min(ANGLE_ARC_RADIUS_PX, Math.hypot(b.x - a.x, b.y - a.y));
      strokeLine(ctx, a, { x: a.x + radius + 16, y: a.y }, theme.textMuted, { width: 1, dash: [4, 4] });
      const direction = Math.atan2(b.y - a.y, b.x - a.x); // canvas angle, so negative is up
      const upward = direction < 0;
      ctx.strokeStyle = theme.accent;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.arc(a.x, a.y, radius, 0, direction, upward);
      ctx.stroke();
      // Just past the arc, on the other side of the horizontal from the line.
      ctx.font = font(LABEL_SIZE_PX, theme);
      ctx.fillStyle = theme.text;
      ctx.textAlign = "left";
      ctx.textBaseline = upward ? "top" : "bottom";
      ctx.fillText(formatAngle(screenAngle(a, b)), a.x + radius + 4, a.y + (upward ? 4 : -4));
    },
    hit: hitSegment,
  },

  "horizontal-line": {
    anchors: 1,
    priceAxisLabel: true,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, { x: 0, y: pts[0].y }, { x: size.width, y: pts[0].y }, theme.accent);
    },
    hit({ pts }, x, y) {
      return Math.abs(y - pts[0].y) <= HIT_TOLERANCE_PX;
    },
  },

  "horizontal-ray": {
    anchors: 1,
    priceAxisLabel: true,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, pts[0], { x: size.width, y: pts[0].y }, theme.accent);
    },
    hit({ pts }, x, y) {
      return x >= pts[0].x - HIT_TOLERANCE_PX && Math.abs(y - pts[0].y) <= HIT_TOLERANCE_PX;
    },
  },

  "vertical-line": {
    anchors: 1,
    timeAxisLabel: true,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, { x: pts[0].x, y: 0 }, { x: pts[0].x, y: size.height }, theme.accent);
    },
    hit({ pts }, x) {
      return Math.abs(x - pts[0].x) <= HIT_TOLERANCE_PX;
    },
  },

  "cross-line": {
    anchors: 1,
    priceAxisLabel: true,
    timeAxisLabel: true,
    draw(ctx, { pts, size, theme }) {
      strokeLine(ctx, { x: 0, y: pts[0].y }, { x: size.width, y: pts[0].y }, theme.accent);
      strokeLine(ctx, { x: pts[0].x, y: 0 }, { x: pts[0].x, y: size.height }, theme.accent);
    },
    hit({ pts }, x, y) {
      return Math.abs(y - pts[0].y) <= HIT_TOLERANCE_PX || Math.abs(x - pts[0].x) <= HIT_TOLERANCE_PX;
    },
  },

  rectangle: {
    anchors: 2,
    draw(ctx, { pts, theme }) {
      const r = bounds(pts[0], pts[1]);
      ctx.fillStyle = theme.accentSoft;
      ctx.fillRect(r.left, r.top, r.right - r.left, r.bottom - r.top);
      ctx.strokeStyle = theme.accent;
      ctx.lineWidth = LINE_WIDTH;
      ctx.strokeRect(r.left, r.top, r.right - r.left, r.bottom - r.top);
    },
    hit: hitBox,
  },

  "fib-retracement": {
    anchors: 2,
    draw(ctx, context) {
      const { pts, theme, coords } = context;
      const { left, right } = bounds(pts[0], pts[1]);
      strokeLine(ctx, pts[0], pts[1], theme.textMuted, { width: 1, dash: [4, 4] });
      ctx.font = font(LABEL_SIZE_PX, theme);
      ctx.textAlign = "left";
      ctx.textBaseline = "bottom";
      for (const { level, price, y, colorKey } of fibLevels(context)) {
        const color = theme[colorKey];
        strokeLine(ctx, { x: left, y }, { x: right, y }, color, { width: 1 });
        ctx.fillStyle = color;
        ctx.fillText(`${level} (${coords.formatPrice(price)})`, left + 4, y - 2);
      }
    },
    hit(context, x, y) {
      const { pts } = context;
      if (distanceToLine(x, y, pts[0], pts[1]) <= HIT_TOLERANCE_PX) return true;
      const { left, right } = bounds(pts[0], pts[1]);
      if (x < left - HIT_TOLERANCE_PX || x > right + HIT_TOLERANCE_PX) return false;
      return fibLevels(context).some((level) => Math.abs(y - level.y) <= HIT_TOLERANCE_PX);
    },
  },

  text: {
    anchors: 1,
    draw(ctx, context) {
      const { drawing, pts, selected, theme } = context;
      ctx.font = font(TEXT_SIZE_PX, theme);
      ctx.textAlign = "left";
      ctx.textBaseline = "top";
      ctx.fillStyle = theme.text;
      ctx.fillText(drawing.text, pts[0].x, pts[0].y);
      if (selected) {
        const r = textBounds(context);
        ctx.strokeStyle = theme.accent;
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.strokeRect(r.left - 3, r.top - 3, r.right - r.left + 6, r.bottom - r.top + 6);
        ctx.setLineDash([]);
      }
    },
    hit(context, x, y) {
      return contains(textBounds(context), x, y, 3);
    },
  },

  "price-range": {
    anchors: 2,
    draw(ctx, context) {
      const { pts, theme } = context;
      const r = bounds(pts[0], pts[1]);
      const { up, text } = priceChange(context);
      const color = up ? theme.up : theme.down;
      const middle = (r.left + r.right) / 2;
      ctx.fillStyle = up ? theme.upSoft : theme.downSoft;
      ctx.fillRect(r.left, r.top, r.right - r.left, r.bottom - r.top);
      for (const { y } of pts) strokeLine(ctx, { x: r.left, y }, { x: r.right, y }, color, { width: 1 });
      drawArrow(ctx, { x: middle, y: pts[0].y }, { x: middle, y: pts[1].y }, color);
      // On the side the price went: above the box for a rise, below for a fall.
      drawLabel(ctx, theme, [text], {
        x: middle,
        y: up ? r.top - LABEL_GAP_PX : r.bottom + LABEL_GAP_PX,
        baseline: up ? "bottom" : "top",
        background: color,
        color: theme.onAccent,
      });
    },
    hit: hitBox,
  },

  "date-range": {
    anchors: 2,
    draw(ctx, context) {
      const { pts, theme } = context;
      const r = bounds(pts[0], pts[1]);
      const middle = (r.top + r.bottom) / 2;
      ctx.fillStyle = theme.accentSoft;
      ctx.fillRect(r.left, r.top, r.right - r.left, r.bottom - r.top);
      for (const { x } of pts) strokeLine(ctx, { x, y: r.top }, { x, y: r.bottom }, theme.accent, { width: 1 });
      drawArrow(ctx, { x: pts[0].x, y: middle }, { x: pts[1].x, y: middle }, theme.accent);
      drawLabel(ctx, theme, [timeSpan(context)], {
        x: (r.left + r.right) / 2,
        y: r.bottom + LABEL_GAP_PX,
        background: theme.accent,
        color: theme.onAccent,
      });
    },
    hit: hitBox,
  },

  // "Date and price range" on the toolbar. Keeps the key it was first
  // saved under, so drawings made before the rename still load.
  measure: {
    anchors: 2,
    draw(ctx, context) {
      const { pts, theme } = context;
      const r = bounds(pts[0], pts[1]);
      const { up, text } = priceChange(context);
      const color = up ? theme.up : theme.down;
      ctx.fillStyle = up ? theme.upSoft : theme.downSoft;
      ctx.fillRect(r.left, r.top, r.right - r.left, r.bottom - r.top);
      strokeLine(ctx, pts[0], pts[1], color, { width: 1, dash: [4, 4] });
      // On the side the price went: above the box for a rise, below for a fall.
      drawLabel(ctx, theme, [text, timeSpan(context)], {
        x: (r.left + r.right) / 2,
        y: up ? r.top - LABEL_GAP_PX : r.bottom + LABEL_GAP_PX,
        baseline: up ? "bottom" : "top",
        background: color,
        color: theme.onAccent,
      });
    },
    hit: hitBox,
  },

  "long-position": positionTool(1),

  "short-position": positionTool(-1),

  "anchored-vwap": {
    anchors: 1,
    priceAxisLabel: (context) => anchoredVwapLine(context).at(-1)?.value ?? null,
    // On the line where it starts, not at the price that happened to be clicked.
    handles(context) {
      const [start] = anchoredVwapLine(context);
      return [start ?? context.pts[0]];
    },
    draw(ctx, context) {
      const path = strokablePath(anchoredVwapLine(context));
      if (path.length === 0) return;
      ctx.strokeStyle = context.theme.accent;
      ctx.lineWidth = LINE_WIDTH;
      ctx.lineJoin = "round";
      ctx.beginPath();
      path.forEach((point, i) => (i === 0 ? ctx.moveTo(point.x, point.y) : ctx.lineTo(point.x, point.y)));
      ctx.stroke();
    },
    hit(context, x, y) {
      const path = strokablePath(anchoredVwapLine(context));
      return path.some((point, i) => i > 0 && distanceToLine(x, y, path[i - 1], point) <= HIT_TOLERANCE_PX);
    },
  },
};
