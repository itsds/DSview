/**
 * useLastUsedTools.js
 *
 * Which tool was last picked from each drawing-toolbar group — what that
 * group's button shows (see components/layout/drawingToolGroups.js) —
 * persisted to localStorage so a reload keeps it, as TradingView does.
 *
 * Stored as { [groupId]: toolId }. Ids aren't checked here: shownTool()
 * ignores any that no longer name a tool in the group.
 *
 * Author: @DS
 */

import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "dsview.drawingToolbar.lastUsed";

function load() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}");
    return saved && typeof saved === "object" && !Array.isArray(saved) ? saved : {};
  } catch {
    return {}; // storage blocked, or a corrupt entry: every group starts on its default
  }
}

export function useLastUsedTools() {
  const [lastUsed, setLastUsed] = useState(load);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(lastUsed));
    } catch {
      // Storage full or blocked: remember them for this session anyway.
    }
  }, [lastUsed]);

  const remember = useCallback(
    (groupId, toolId) => setLastUsed((saved) => (saved[groupId] === toolId ? saved : { ...saved, [groupId]: toolId })),
    [],
  );

  return { lastUsed, remember };
}
