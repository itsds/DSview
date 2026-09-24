/**
 * ToolbarButton.jsx
 *
 * The one button used by every toolbar, plus ToolbarDivider.
 *
 * Controls for features that don't exist yet are still rendered (so the
 * layout is final) but marked `notBuilt`: dimmed, inert, and their tooltip
 * says so. That uses aria-disabled rather than the disabled attribute — a
 * truly disabled button gets no mouse events, so some browsers never show
 * its tooltip.
 *
 * `pressed` is for toggles (a timeframe, a drawing tool); leave it
 * undefined on plain action buttons so screen readers don't announce them
 * as toggles.
 *
 * Author: @DS
 */

import styles from "./ToolbarButton.module.css";

export default function ToolbarButton({ icon: Icon, label, title, pressed, notBuilt = false, onClick }) {
  const tooltip = notBuilt ? `${title} — not built yet` : title;
  const className = [styles.button, !label && styles.iconOnly, pressed && styles.pressed]
    .filter(Boolean)
    .join(" ");

  return (
    <button
      type="button"
      className={className}
      title={tooltip}
      aria-label={label ? undefined : tooltip}
      aria-pressed={pressed}
      aria-disabled={notBuilt || undefined}
      onClick={notBuilt ? undefined : onClick}
    >
      {Icon && <Icon size={18} strokeWidth={1.75} aria-hidden="true" />}
      {label && <span>{label}</span>}
    </button>
  );
}

export function ToolbarDivider({ orientation = "vertical" }) {
  return <span role="separator" aria-orientation={orientation} className={styles[orientation]} />;
}
