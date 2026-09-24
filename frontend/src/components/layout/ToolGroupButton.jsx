/**
 * ToolGroupButton.jsx
 *
 * One group on the drawing toolbar, TradingView-style: a button showing
 * the group's current tool (drawingToolGroups.js's shownTool) that picks
 * it, and — when the group has more than one tool — a narrow arrow beside
 * it that opens ToolMenu.jsx with the whole group. Which menu is open is
 * DrawingToolbar.jsx's state, so only one is ever open.
 *
 * Author: @DS
 */

import { ChevronRight } from "lucide-react";
import { useRef } from "react";

import { toolTooltip, toolsOf } from "./drawingToolGroups.js";
import styles from "./ToolGroupButton.module.css";
import ToolbarButton from "./ToolbarButton.jsx";
import ToolMenu from "./ToolMenu.jsx";

export default function ToolGroupButton({ group, tool, activeTool, open, onOpenChange, onSelect }) {
  const groupRef = useRef(null);
  const toggleRef = useRef(null);
  const tools = toolsOf(group);
  const menuLabel = `All ${group.title.toLowerCase()}`;

  const closeMenu = ({ restoreFocus = false } = {}) => {
    onOpenChange(false);
    if (restoreFocus) toggleRef.current?.focus();
  };

  return (
    <div ref={groupRef} className={styles.group}>
      <ToolbarButton
        icon={tool.icon}
        title={toolTooltip(tool)}
        pressed={tools.some((t) => t.id === activeTool)}
        onClick={() => onSelect(tool.id)}
      />
      {tools.length > 1 && (
        <button
          ref={toggleRef}
          type="button"
          className={styles.toggle}
          title={menuLabel}
          aria-label={menuLabel}
          aria-haspopup="menu"
          aria-expanded={open}
          onClick={() => onOpenChange(!open)}
        >
          <ChevronRight size={12} strokeWidth={2} aria-hidden="true" />
        </button>
      )}
      {open && (
        <ToolMenu
          group={group}
          anchorRef={groupRef}
          currentTool={tool.id}
          label={menuLabel}
          onSelect={onSelect}
          onClose={closeMenu}
        />
      )}
    </div>
  );
}
