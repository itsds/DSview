/**
 * IndicatorsDialog.jsx
 *
 * The "Indicators" picker: a searchable list of every indicator in
 * indicators/definitions.js. Picking one adds it with default settings and
 * closes the dialog.
 *
 * A native <dialog> opened with showModal(), so focus trapping, Esc to
 * close and the backdrop come from the browser. Mounted only while open
 * (App.jsx renders it conditionally), so every opening starts fresh.
 *
 * Author: @DS
 */

import { X } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { INDICATORS, MAX_PRICE_OVERLAYS } from "../../indicators/definitions.js";
import styles from "./dialog.module.css";

export default function IndicatorsDialog({ onAdd, canAdd, onClose }) {
  const dialogRef = useRef(null);
  const searchRef = useRef(null);
  const [query, setQuery] = useState("");

  useEffect(() => {
    // No cleanup that closes it: StrictMode's rehearsal unmount would fire onClose.
    if (!dialogRef.current.open) dialogRef.current.showModal();
    searchRef.current.focus();
  }, []);

  const needle = query.trim().toLowerCase();
  const matches = Object.entries(INDICATORS).filter(([, def]) =>
    `${def.name} ${def.shortName}`.toLowerCase().includes(needle),
  );

  return (
    <dialog
      ref={dialogRef}
      className={styles.dialog}
      aria-labelledby="indicators-title"
      onClose={onClose}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose(); // the backdrop
      }}
    >
      <div className={styles.content}>
        <div className={styles.header}>
          <h2 id="indicators-title" className={styles.title}>
            Indicators
          </h2>
          <button type="button" className={styles.close} aria-label="Close" onClick={onClose}>
            <X size={18} aria-hidden="true" />
          </button>
        </div>
        <input
          ref={searchRef}
          type="search"
          className={`${styles.input} ${styles.search}`}
          placeholder="Search"
          aria-label="Search indicators"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <ul className={styles.list}>
          {matches.map(([type, def]) => {
            const allowed = canAdd(type);
            return (
              <li key={type}>
                <button
                  type="button"
                  className={styles.item}
                  disabled={!allowed}
                  onClick={() => {
                    onAdd(type);
                    onClose();
                  }}
                >
                  <span className={styles.itemName}>
                    {def.name}
                    <span className={styles.shortName}>{def.shortName}</span>
                  </span>
                  <span className={styles.itemMeta}>
                    {allowed
                      ? `${def.pane === "price" ? "On the chart" : "Own pane"} · ${def.description}`
                      : `Limit reached: ${MAX_PRICE_OVERLAYS} indicators on the chart at once`}
                  </span>
                </button>
              </li>
            );
          })}
          {matches.length === 0 && <li className={styles.empty}>No indicators match “{query.trim()}”.</li>}
        </ul>
      </div>
    </dialog>
  );
}
