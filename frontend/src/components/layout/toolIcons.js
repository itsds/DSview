/**
 * toolIcons.js
 *
 * Drawing-tool icons lucide doesn't have. Drawn for DSView — never copied
 * from TradingView's artwork — on lucide's 24px grid and built with its
 * createLucideIcon, so they take the same props as lucide's own icons and
 * share their stroke style. Anchor points are small hollow circles, like
 * the handles on a selected drawing.
 *
 * Author: @DS
 */

import { createLucideIcon } from "lucide-react";

const anchor = (cx, cy) => ["circle", { cx, cy, r: 2 }];
const path = (d, attrs = {}) => ["path", { d, ...attrs }];

function toolIcon(name, nodes) {
  // React wants a key on every node; lucide's own icons ship with theirs.
  return createLucideIcon(
    name,
    nodes.map(([tag, attrs], i) => [tag, { ...attrs, key: `${name}-${i}` }]),
  );
}

// Lines
export const TrendLine = toolIcon("trend-line", [anchor(5, 19), anchor(19, 5), path("M6.4 17.6 17.6 6.4")]);
export const Ray = toolIcon("ray", [
  anchor(5, 19),
  anchor(12, 12),
  path("M6.4 17.6 10.6 13.4"),
  path("M13.4 10.6 21 3"),
]);
export const InfoLine = toolIcon("info-line", [
  anchor(4, 16),
  anchor(14, 6),
  path("M5.4 14.6 12.6 7.4"),
  ["rect", { x: 13, y: 14, width: 8, height: 7, rx: 1 }],
  path("M15.5 17.5h3"),
]);
export const ExtendedLine = toolIcon("extended-line", [
  anchor(9, 15),
  anchor(15, 9),
  path("M3 21 7.6 16.4"),
  path("M10.4 13.6 13.6 10.4"),
  path("M16.4 7.6 21 3"),
]);
export const TrendAngle = toolIcon("trend-angle", [
  anchor(4, 18),
  path("M5.6 16.8 20 6"),
  path("M6 18h15"),
  path("M12 18a8 8 0 0 0-1.6-4.8"),
]);
export const HorizontalLine = toolIcon("horizontal-line", [anchor(12, 12), path("M2 12h8"), path("M14 12h8")]);
export const HorizontalRay = toolIcon("horizontal-ray", [anchor(5, 12), path("M7 12h15")]);
export const VerticalLine = toolIcon("vertical-line", [anchor(12, 12), path("M12 2v8"), path("M12 14v8")]);
export const CrossLine = toolIcon("cross-line", [
  anchor(12, 12),
  path("M2 12h8"),
  path("M14 12h8"),
  path("M12 2v8"),
  path("M12 14v8"),
]);

// Measurers: an arrow across whatever the tool measures.
export const PriceRange = toolIcon("price-range", [
  path("M5 3h14"),
  path("M5 21h14"),
  path("M12 6v12"),
  path("m9 9 3-3 3 3"),
  path("m9 15 3 3 3-3"),
]);
export const DateRange = toolIcon("date-range", [
  path("M3 5v14"),
  path("M21 5v14"),
  path("M6 12h12"),
  path("m9 9-3 3 3 3"),
  path("m15 9 3 3-3 3"),
]);
export const DateAndPriceRange = toolIcon("date-and-price-range", [
  ["rect", { x: 3, y: 3, width: 18, height: 18, rx: 2 }],
  path("M8 16 16 8"),
  path("M11 8h5v5"),
]);

// Positions: the entry line between target and stop zones, and an arrow
// the way the trade goes.
export const LongPosition = toolIcon("long-position", [
  ["rect", { x: 3, y: 3, width: 18, height: 18, rx: 2 }],
  path("M3 15h18"),
  path("M12 12V6"),
  path("m9 9 3-3 3 3"),
]);
export const ShortPosition = toolIcon("short-position", [
  ["rect", { x: 3, y: 3, width: 18, height: 18, rx: 2 }],
  path("M3 9h18"),
  path("M12 12v6"),
  path("m9 15 3 3 3-3"),
]);

// Channels and pitchforks: not built yet (roadmap step 3). The four
// pitchforks share one icon until then.
export const ParallelChannel = toolIcon("parallel-channel", [path("M3 14 14 3"), path("M10 21 21 10")]);
export const RegressionTrend = toolIcon("regression-trend", [
  path("M3 13 13 3"),
  path("M11 21 21 11"),
  path("M7 17 17 7", { strokeDasharray: "2 3" }),
]);
export const FlatTopBottom = toolIcon("flat-top-bottom", [path("M3 5h18"), path("M3 20 21 11")]);
export const DisjointChannel = toolIcon("disjoint-channel", [path("M3 9 21 3"), path("M3 15 21 21")]);
export const Pitchfork = toolIcon("pitchfork", [
  path("M3 21 21 3"),
  path("M7 9 13 3"),
  path("M15 17 21 11"),
  path("M7 9 15 17"),
]);
