/**
 * drawingToolGroups.js
 *
 * The drawing toolbar's layout, TradingView-style: tools in groups, each
 * group one button showing its current tool, with a pop-out menu of the
 * whole group in titled sections. Also each tool's name, icon and keyboard
 * shortcut.
 *
 * Tool ids are keys of drawing/tools.js's TOOLS (the crosshair aside).
 * Tools marked `notBuilt` are on the agreed roadmap and are listed dimmed
 * and inert, the same convention as ToolbarButton.jsx's `notBuilt`: when
 * one lands, add it to TOOLS and drop the flag here.
 *
 * Shortcuts are Alt + a letter (lib/keyboard.js). The line and Fibonacci
 * ones use the same letters as TradingView.
 *
 * Author: @DS
 */

import { Anchor, ChartBar, ChartBarBig, Crosshair, Square, TextAlignJustify, Type } from "lucide-react";

import { CROSSHAIR_TOOL } from "../../drawing/tools.js";
import { formatShortcut, matchesShortcut } from "../../lib/keyboard.js";
import * as toolIcons from "./toolIcons.js";

export const TOOL_GROUPS = [
  {
    id: "cursors",
    title: "Cursors",
    sections: [{ tools: [{ id: CROSSHAIR_TOOL, title: "Crosshair", icon: Crosshair }] }],
  },
  {
    id: "lines",
    title: "Line tools",
    sections: [
      {
        title: "Lines",
        tools: [
          { id: "trend-line", title: "Trend line", icon: toolIcons.TrendLine, shortcut: { key: "T" } },
          { id: "ray", title: "Ray", icon: toolIcons.Ray },
          { id: "info-line", title: "Info line", icon: toolIcons.InfoLine },
          { id: "extended-line", title: "Extended line", icon: toolIcons.ExtendedLine },
          { id: "trend-angle", title: "Trend angle", icon: toolIcons.TrendAngle },
          { id: "horizontal-line", title: "Horizontal line", icon: toolIcons.HorizontalLine, shortcut: { key: "H" } },
          { id: "horizontal-ray", title: "Horizontal ray", icon: toolIcons.HorizontalRay, shortcut: { key: "J" } },
          { id: "vertical-line", title: "Vertical line", icon: toolIcons.VerticalLine, shortcut: { key: "V" } },
          { id: "cross-line", title: "Cross line", icon: toolIcons.CrossLine, shortcut: { key: "C" } },
        ],
      },
      {
        title: "Channels",
        tools: [
          { id: "parallel-channel", title: "Parallel channel", icon: toolIcons.ParallelChannel, notBuilt: true },
          { id: "regression-trend", title: "Regression trend", icon: toolIcons.RegressionTrend, notBuilt: true },
          { id: "flat-top-bottom", title: "Flat top/bottom", icon: toolIcons.FlatTopBottom, notBuilt: true },
          { id: "disjoint-channel", title: "Disjoint channel", icon: toolIcons.DisjointChannel, notBuilt: true },
        ],
      },
      {
        title: "Pitchforks",
        tools: [
          { id: "pitchfork", title: "Pitchfork", icon: toolIcons.Pitchfork, notBuilt: true },
          { id: "schiff-pitchfork", title: "Schiff pitchfork", icon: toolIcons.Pitchfork, notBuilt: true },
          {
            id: "modified-schiff-pitchfork",
            title: "Modified Schiff pitchfork",
            icon: toolIcons.Pitchfork,
            notBuilt: true,
          },
          { id: "inside-pitchfork", title: "Inside pitchfork", icon: toolIcons.Pitchfork, notBuilt: true },
        ],
      },
    ],
  },
  {
    id: "fibonacci",
    title: "Fibonacci tools",
    sections: [
      {
        tools: [
          { id: "fib-retracement", title: "Fibonacci retracement", icon: TextAlignJustify, shortcut: { key: "F" } },
        ],
      },
    ],
  },
  {
    id: "measurement",
    title: "Forecasting and measurement tools",
    defaultTool: "measure", // what this button was before the toolbar had groups
    sections: [
      {
        title: "Projection",
        tools: [
          { id: "long-position", title: "Long position", icon: toolIcons.LongPosition },
          { id: "short-position", title: "Short position", icon: toolIcons.ShortPosition },
        ],
      },
      {
        title: "Volume-based",
        tools: [
          { id: "anchored-vwap", title: "Anchored VWAP", icon: Anchor },
          { id: "fixed-range-volume-profile", title: "Fixed range volume profile", icon: ChartBar, notBuilt: true },
          { id: "anchored-volume-profile", title: "Anchored volume profile", icon: ChartBarBig, notBuilt: true },
        ],
      },
      {
        title: "Measurers",
        tools: [
          { id: "price-range", title: "Price range", icon: toolIcons.PriceRange },
          { id: "date-range", title: "Date range", icon: toolIcons.DateRange },
          { id: "measure", title: "Date and price range", icon: toolIcons.DateAndPriceRange },
        ],
      },
    ],
  },
  {
    id: "shapes",
    title: "Shapes",
    sections: [{ tools: [{ id: "rectangle", title: "Rectangle", icon: Square, shortcut: { key: "R", shift: true } }] }],
  },
  {
    id: "annotation",
    title: "Annotation tools",
    sections: [{ tools: [{ id: "text", title: "Text", icon: Type }] }],
  },
];

/** Every tool in a group, in menu order. */
export function toolsOf(group) {
  return group.sections.flatMap((section) => section.tools);
}

export function groupOf(toolId) {
  return TOOL_GROUPS.find((group) => toolsOf(group).some((tool) => tool.id === toolId)) ?? null;
}

/**
 * The tool a group's button shows: the active tool if it's one of the
 * group's, else the one last picked from the group, else its default.
 */
export function shownTool(group, activeTool, lastUsedId) {
  const built = toolsOf(group).filter((tool) => !tool.notBuilt);
  return (
    built.find((tool) => tool.id === activeTool) ??
    built.find((tool) => tool.id === lastUsedId) ??
    built.find((tool) => tool.id === group.defaultTool) ??
    built[0]
  );
}

/** The built tool whose shortcut a keydown is, if any. */
export function toolForShortcut(event) {
  for (const group of TOOL_GROUPS) {
    const tool = toolsOf(group).find((t) => !t.notBuilt && t.shortcut && matchesShortcut(event, t.shortcut));
    if (tool) return tool;
  }
  return null;
}

/** A tool's tooltip: "Trend line (Alt + T)". */
export function toolTooltip(tool) {
  return tool.shortcut ? `${tool.title} (${formatShortcut(tool.shortcut)})` : tool.title;
}
