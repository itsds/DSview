/**
 * ToolMenu.jsx
 *
 * A drawing-toolbar group's pop-out menu: the group's tools under their
 * section headings, each with its shortcut, the current one marked.
 * `notBuilt` tools are listed dimmed and inert, as in ToolbarButton.jsx.
 *
 * Portalled to <body> and positioned `fixed` beside the group's button:
 * rendered inside the toolbar, which scrolls (overflow-y: auto), it would
 * be clipped.
 *
 * Keyboard, as for any menu button: it opens with focus on the current
 * tool; arrows, Home and End move; Enter or Space picks; Esc or Tab closes
 * it and puts focus back on the arrow. Esc is caught wherever focus is and
 * marked handled with preventDefault(), so the drawing controller doesn't
 * also take it as "leave the tool" (see lib/keyboard.js's isChartKey). A
 * press anywhere outside the menu closes it too.
 *
 * Author: @DS
 */

import { useEffect, useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";

import { formatShortcut } from "../../lib/keyboard.js";
import styles from "./ToolMenu.module.css";

const GAP_PX = 8; // between the group's button and the menu
const MARGIN_PX = 8; // kept clear of the window's edges

function menuItems(menu) {
  return [...menu.querySelectorAll("[role='menuitemradio']")];
}

export default function ToolMenu({ group, anchorRef, currentTool, label, onSelect, onClose }) {
  const menuRef = useRef(null);
  // The latest onClose, for listeners that are only added once.
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  // Placed and focused before the first paint, so it never shows in the wrong spot.
  useLayoutEffect(() => {
    const menu = menuRef.current;
    const anchor = anchorRef.current.getBoundingClientRect();
    const lowestTop = window.innerHeight - menu.offsetHeight - MARGIN_PX;
    menu.style.left = `${anchor.right + GAP_PX}px`;
    menu.style.top = `${Math.max(MARGIN_PX, Math.min(anchor.top, lowestTop))}px`;
    const items = menuItems(menu);
    (items.find((item) => item.dataset.tool === currentTool) ?? items[0])?.focus({ preventScroll: true });
  }, [anchorRef, currentTool]);

  useEffect(() => {
    const close = (options) => onCloseRef.current(options);
    const handlePointerDown = (event) => {
      if (!menuRef.current.contains(event.target) && !anchorRef.current.contains(event.target)) close();
    };
    const handleKeyDown = (event) => {
      if (event.key !== "Escape") return;
      event.preventDefault(); // handled: see the file comment
      close({ restoreFocus: true });
    };
    const handleResize = () => close();
    // Capture phase, so nothing further down (like the drawing controller
    // stopping a press on a drawing) can keep these from us.
    document.addEventListener("pointerdown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeyDown, true);
    window.addEventListener("resize", handleResize);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown, true);
      document.removeEventListener("keydown", handleKeyDown, true);
      window.removeEventListener("resize", handleResize);
    };
  }, [anchorRef]);

  const handleKeyDown = (event) => {
    const items = menuItems(menuRef.current);
    const index = items.indexOf(document.activeElement);
    const focusAt = (i) => items[(i + items.length) % items.length].focus();
    if (event.key === "ArrowDown") focusAt(index + 1);
    else if (event.key === "ArrowUp") focusAt(index < 0 ? -1 : index - 1);
    else if (event.key === "Home") focusAt(0);
    else if (event.key === "End") focusAt(items.length - 1);
    else if (event.key === "Tab") onClose({ restoreFocus: true });
    else return;
    event.preventDefault();
  };

  return createPortal(
    <div ref={menuRef} role="menu" aria-label={label} className={styles.menu} onKeyDown={handleKeyDown}>
      {group.sections.map((section, i) => (
        <div key={section.title ?? i} role="group" aria-label={section.title} className={styles.section}>
          {section.title && (
            <div className={styles.heading} aria-hidden="true">
              {section.title}
            </div>
          )}
          {section.tools.map((tool) => {
            const Icon = tool.icon;
            return (
              <button
                key={tool.id}
                type="button"
                role="menuitemradio"
                aria-checked={tool.id === currentTool}
                aria-disabled={tool.notBuilt || undefined}
                tabIndex={-1}
                data-tool={tool.id}
                className={styles.item}
                title={tool.notBuilt ? `${tool.title} — not built yet` : undefined}
                onClick={tool.notBuilt ? undefined : () => onSelect(tool.id)}
              >
                <Icon size={18} strokeWidth={1.75} aria-hidden="true" />
                <span className={styles.name}>{tool.title}</span>
                {tool.shortcut && <kbd className={styles.shortcut}>{formatShortcut(tool.shortcut)}</kbd>}
              </button>
            );
          })}
        </div>
      ))}
    </div>,
    document.body,
  );
}
