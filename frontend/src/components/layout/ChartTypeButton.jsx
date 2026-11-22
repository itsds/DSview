/**
 * ChartTypeButton.jsx
 *
 * The top bar's chart type button: shows the current type and opens a
 * menu (ToolMenu.jsx, below the button) to switch between candles and the
 * volume footprint, and to set the footprint's row size — "Auto" fits the
 * rows to the zoom, a fixed size keeps them that wide (TradingView's
 * "ticks per row"). The footprint exists on 1m to 1h; on 4h and 1D the
 * chart shows candles, and the menu says so.
 *
 * Author: @DS
 */

import { ChartCandlestick, Grid3x3 } from "lucide-react";
import { useRef, useState } from "react";

import { FOOTPRINT_TIMEFRAMES, ROW_SIZES } from "../../footprint/levels.js";
import ToolbarButton from "./ToolbarButton.jsx";
import ToolMenu from "./ToolMenu.jsx";

const TYPES = {
  candles: { title: "Candles", icon: ChartCandlestick },
  footprint: { title: "Footprint", icon: Grid3x3 },
};

function menuGroup(timeframe) {
  const footprintHere = FOOTPRINT_TIMEFRAMES.includes(timeframe);
  return {
    sections: [
      {
        title: "Chart type",
        tools: [
          { id: "type:candles", title: TYPES.candles.title, icon: TYPES.candles.icon },
          {
            id: "type:footprint",
            title: footprintHere ? TYPES.footprint.title : "Footprint (1m–1h only)",
            icon: TYPES.footprint.icon,
          },
        ],
      },
      {
        title: "Footprint rows",
        tools: [
          { id: "rows:auto", title: "Auto (fits the zoom)" },
          ...ROW_SIZES.map((size) => ({ id: "rows:" + size, title: "$" + size })),
        ],
      },
    ],
  };
}

export default function ChartTypeButton({ chartType, footprintRows, timeframe, onChartTypeChange, onFootprintRowsChange }) {
  const [open, setOpen] = useState(false);
  const anchorRef = useRef(null);
  const current = TYPES[chartType];

  const close = ({ restoreFocus = false } = {}) => {
    setOpen(false);
    if (restoreFocus) anchorRef.current?.querySelector("button")?.focus();
  };

  const select = (id) => {
    const [kind, value] = id.split(":");
    if (kind === "type") onChartTypeChange(value);
    else onFootprintRowsChange(value === "auto" ? "auto" : Number(value));
    close({ restoreFocus: true });
  };

  return (
    <span ref={anchorRef} style={{ display: "inline-flex" }}>
      <ToolbarButton
        icon={current.icon}
        title={`Chart type: ${current.title}`}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      />
      {open && (
        <ToolMenu
          group={menuGroup(timeframe)}
          anchorRef={anchorRef}
          currentTool={`type:${chartType}`}
          isChecked={(tool) => tool.id === `type:${chartType}` || tool.id === `rows:${footprintRows}`}
          label="Chart type"
          placement="below"
          onSelect={select}
          onClose={close}
        />
      )}
    </span>
  );
}
