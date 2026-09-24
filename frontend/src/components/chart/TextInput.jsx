/**
 * TextInput.jsx
 *
 * Inline editor for the text drawing tool: an input placed where the user
 * clicked on the chart. Enter or clicking away keeps the text; Esc
 * discards it. Reports exactly once, however it closes — Enter unmounts
 * the input, and some browsers fire a blur on that too.
 *
 * Author: @DS
 */

import { useEffect, useRef } from "react";

import styles from "./TextInput.module.css";

export default function TextInput({ x, y, onDone }) {
  const inputRef = useRef(null);
  const doneRef = useRef(false);

  const finish = (text) => {
    if (doneRef.current) return;
    doneRef.current = true;
    onDone(text);
  };

  useEffect(() => {
    inputRef.current.focus();
  }, []);

  return (
    <input
      ref={inputRef}
      className={styles.input}
      style={{ left: x, top: y }}
      aria-label="Drawing text"
      placeholder="Text"
      onKeyDown={(event) => {
        if (event.key === "Enter") finish(event.currentTarget.value);
        else if (event.key === "Escape") finish(null);
      }}
      onBlur={(event) => finish(event.currentTarget.value)}
    />
  );
}
