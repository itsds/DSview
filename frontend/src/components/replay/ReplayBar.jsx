/**
 * ReplayBar.jsx
 *
 * The replay's controls, floating over the bottom of the chart while a
 * replay is under way (Phase 4a): what it's doing and the replay's time
 * (UTC, like the chart's axis), play/pause, the speed, "Select bar" to
 * start again from another bar, and the exit, which stops the replay and
 * takes the chart back to live.
 *
 * Author: @DS
 */

import { MousePointerClick, Pause, Play, X } from "lucide-react";

import { SPEEDS, speedLabel } from "../../api/replay.js";
import styles from "./ReplayBar.module.css";

/** "2026-11-20 10:42:15 UTC" for an ISO timestamp. */
export function replayTimeLabel(iso) {
  if (!iso) return "";
  return `${new Date(iso).toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

/** What the bar says the replay is doing. */
export function replayStateLabel(status) {
  switch (status?.state) {
    case "starting":
      return `Starting: ${status.phase ?? "preparing"}…`;
    case "playing":
      return "Replaying";
    case "paused":
      return "Paused";
    case "finished":
      return "End of the history";
    case "failed":
      return `Replay failed: ${status.error ?? "unknown error"}`;
    default:
      return "";
  }
}

export default function ReplayBar({ status, speed, error, onPlay, onPause, onSpeed, onSelectBar, onExit }) {
  const state = status?.state;
  const canPlay = state === "paused" || state === "starting";
  const playing = state === "playing";
  const shownSpeed = status?.speed ?? speed;

  return (
    <div className={styles.bar} role="toolbar" aria-label="Replay">
      <span className={styles.badge}>Replay</span>
      <span className={state === "failed" ? `${styles.state} ${styles.failed}` : styles.state}>
        {replayStateLabel(status)}
      </span>
      {state !== "failed" && <span className={styles.clock}>{replayTimeLabel(status?.clock)}</span>}
      <span className={styles.divider} />
      {playing ? (
        <button type="button" className={styles.button} title="Pause" aria-label="Pause" onClick={onPause}>
          <Pause size={16} aria-hidden="true" />
        </button>
      ) : (
        <button
          type="button"
          className={styles.button}
          title="Play"
          aria-label="Play"
          aria-disabled={!canPlay || undefined}
          onClick={canPlay ? onPlay : undefined}
        >
          <Play size={16} aria-hidden="true" />
        </button>
      )}
      <div className={styles.speeds} role="group" aria-label="Speed">
        {SPEEDS.map((value) => (
          <button
            key={value}
            type="button"
            className={value === shownSpeed ? `${styles.speed} ${styles.current}` : styles.speed}
            aria-pressed={value === shownSpeed}
            title={value === "max" ? "As fast as the pipeline keeps up" : `${value} times real time`}
            onClick={() => onSpeed(value)}
          >
            {speedLabel(value)}
          </button>
        ))}
      </div>
      <span className={styles.divider} />
      <button type="button" className={styles.button} title="Select another bar to replay from" onClick={onSelectBar}>
        <MousePointerClick size={16} aria-hidden="true" />
        <span>Select bar</span>
      </button>
      <button type="button" className={styles.button} title="Exit the replay, back to live" aria-label="Exit replay" onClick={onExit}>
        <X size={16} aria-hidden="true" />
      </button>
      {error && (
        <span className={styles.error} role="alert">
          {error}
        </span>
      )}
    </div>
  );
}
