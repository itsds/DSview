/**
 * IndicatorSettingsDialog.jsx
 *
 * Edits one indicator's parameters (lengths, band width). Opened from the
 * gear on its legend row; mounted only while open and keyed by indicator
 * (App.jsx), so the draft always starts from the indicator's current
 * values. The browser's own number-input validation (min/max/step) blocks
 * bad values, and useIndicators.js clamps whatever gets through.
 *
 * Author: @DS
 */

import { X } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { INDICATORS, indicatorLabel } from "../../indicators/definitions.js";
import styles from "./dialog.module.css";

export default function IndicatorSettingsDialog({ indicator, onSave, onClose }) {
  const def = INDICATORS[indicator.type];
  const dialogRef = useRef(null);
  const formRef = useRef(null);
  const [draft, setDraft] = useState(() =>
    Object.fromEntries(def.params.map((param) => [param.key, String(indicator.params[param.key])])),
  );

  useEffect(() => {
    // No cleanup that closes it: StrictMode's rehearsal unmount would fire onClose.
    if (!dialogRef.current.open) dialogRef.current.showModal();
    formRef.current.querySelector("input")?.focus();
  }, []);

  return (
    <dialog
      ref={dialogRef}
      className={styles.dialog}
      aria-labelledby="indicator-settings-title"
      onClose={onClose}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose(); // the backdrop
      }}
    >
      <div className={styles.content}>
        <div className={styles.header}>
          <h2 id="indicator-settings-title" className={styles.title}>
            {indicatorLabel(indicator)} settings
          </h2>
          <button type="button" className={styles.close} aria-label="Close" onClick={onClose}>
            <X size={18} aria-hidden="true" />
          </button>
        </div>
        <form
          ref={formRef}
          className={styles.form}
          onSubmit={(event) => {
            event.preventDefault();
            onSave(Object.fromEntries(Object.entries(draft).map(([key, value]) => [key, Number(value)])));
            onClose();
          }}
        >
          {def.params.map((param) => (
            <label key={param.key} className={styles.field}>
              <span>{param.label}</span>
              <input
                type="number"
                className={styles.input}
                min={param.min}
                max={param.max}
                step={param.step ?? 1}
                required
                value={draft[param.key]}
                onChange={(event) => setDraft((current) => ({ ...current, [param.key]: event.target.value }))}
              />
            </label>
          ))}
          <div className={styles.buttons}>
            <button type="button" className={styles.button} onClick={onClose}>
              Cancel
            </button>
            <button type="submit" className={`${styles.button} ${styles.primary}`}>
              OK
            </button>
          </div>
        </form>
      </div>
    </dialog>
  );
}
