/**
 * DrawingToolbar.jsx
 *
 * Left-hand column of drawing tools, plus magnet mode and "remove all".
 * Tools come in TradingView-style groups (drawingToolGroups.js): one
 * button per group, showing the tool last picked from it
 * (useLastUsedTools.js), with a pop-out menu of the whole group
 * (ToolGroupButton.jsx) — plus Alt-key shortcuts, listened for here.
 *
 * Controls only: App.jsx owns which tool is active, and
 * drawing/DrawingController.js does the actual placing and editing.
 *
 * Author: @DS
 */

import { Magnet, Trash } from "lucide-react";
import { useCallback, useEffect, useState } from "react";

import { useLastUsedTools } from "../../hooks/useLastUsedTools.js";
import { isChartKey } from "../../lib/keyboard.js";
import { TOOL_GROUPS, groupOf, shownTool, toolForShortcut } from "./drawingToolGroups.js";
import styles from "./DrawingToolbar.module.css";
import ToolbarButton, { ToolbarDivider } from "./ToolbarButton.jsx";
import ToolGroupButton from "./ToolGroupButton.jsx";

export default function DrawingToolbar({ activeTool, onToolChange, magnet, onMagnetChange, onClearDrawings }) {
  const { lastUsed, remember } = useLastUsedTools();
  const [openGroupId, setOpenGroupId] = useState(null);

  // Every way of picking a tool ends here: a group's button, its menu, or a shortcut.
  const select = useCallback(
    (toolId) => {
      remember(groupOf(toolId).id, toolId);
      setOpenGroupId(null);
      onToolChange(toolId);
    },
    [remember, onToolChange],
  );

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (!isChartKey(event)) return;
      const tool = toolForShortcut(event);
      if (!tool) return;
      event.preventDefault(); // or e.g. Alt+F also opens the browser's own menu
      if (!event.repeat) select(tool.id);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [select]);

  return (
    <nav className={styles.toolbar} aria-label="Drawing tools">
      {TOOL_GROUPS.map((group) => (
        <ToolGroupButton
          key={group.id}
          group={group}
          tool={shownTool(group, activeTool, lastUsed[group.id])}
          activeTool={activeTool}
          open={openGroupId === group.id}
          onOpenChange={(open) => setOpenGroupId(open ? group.id : null)}
          onSelect={select}
        />
      ))}
      <ToolbarDivider orientation="horizontal" />
      <ToolbarButton
        icon={Magnet}
        title="Magnet mode (snap to open, high, low, close)"
        pressed={magnet}
        onClick={() => onMagnetChange(!magnet)}
      />
      <ToolbarButton icon={Trash} title="Remove all drawings" onClick={onClearDrawings} />
    </nav>
  );
}
