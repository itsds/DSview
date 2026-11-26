/**
 * replay.js
 *
 * The client for the replay's controls (backend/routers/replay.py): the
 * replay service's status, and the commands that start, steer and stop a
 * replay. Each command is queued for the service and answers at once; the
 * status shows when it took effect.
 *
 * Author: @DS
 */

import { BACKEND_HTTP_URL } from "../config.js";

export const SPEEDS = [1, 10, 100, "max"];

/** The replay service's status: { state, ... }, state "unavailable" when it isn't running. */
export async function fetchReplayStatus() {
  const response = await fetch(`${BACKEND_HTTP_URL}/replay`);
  if (!response.ok) throw new Error(`GET /replay failed: ${response.status} ${response.statusText}`);
  return response.json();
}

async function post(path, body) {
  const response = await fetch(`${BACKEND_HTTP_URL}/replay/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.json().then((json) => json.detail, () => null);
    throw new Error(typeof detail === "string" ? detail : `POST /replay/${path} failed: ${response.status}`);
  }
  return response.json();
}

/** Start a replay of `symbol` from `start` (Unix seconds) at `speed` (a multiple of real time, or "max"). */
export function startReplay({ symbol, start, speed }) {
  return post("start", { symbol, start: new Date(start * 1000).toISOString(), speed });
}

export const pauseReplay = () => post("pause");
export const resumeReplay = () => post("resume");
export const stopReplay = () => post("stop");
export const setReplaySpeed = (speed) => post("speed", { speed });

/** A speed as the replay bar shows it: "10×", "Max". */
export function speedLabel(speed) {
  return speed === "max" ? "Max" : `${speed}×`;
}
