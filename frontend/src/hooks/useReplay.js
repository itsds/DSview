/**
 * useReplay.js
 *
 * The replay service's status, polled every second (api/replay.js), and
 * the commands that start, steer and stop a replay. A command is queued and
 * answered at once; its effect shows in the next status. A command that
 * fails (the service stopped, say) is kept in `error` until the next one
 * succeeds.
 *
 * The speed a replay starts at is the one used last, kept in localStorage.
 *
 * Author: @DS
 */

import { useCallback, useEffect, useState } from "react";

import {
  SPEEDS,
  fetchReplayStatus,
  pauseReplay,
  resumeReplay,
  setReplaySpeed,
  startReplay,
  stopReplay,
} from "../api/replay.js";

const POLL_MS = 1_000;
const SPEED_KEY = "dsview.replaySpeed";
const DEFAULT_SPEED = 10;

function savedSpeed() {
  try {
    const saved = JSON.parse(localStorage.getItem(SPEED_KEY));
    return SPEEDS.includes(saved) ? saved : DEFAULT_SPEED;
  } catch {
    return DEFAULT_SPEED;
  }
}

export function useReplay() {
  const [status, setStatus] = useState(null); // null until the first answer
  const [error, setError] = useState(null);
  const [speed, setSpeed] = useState(savedSpeed);

  const refresh = useCallback(async () => {
    try {
      setStatus(await fetchReplayStatus());
    } catch {
      setStatus({ state: "unavailable" }); // the backend itself is down
    }
  }, []);

  useEffect(() => {
    let timer;
    let cancelled = false;
    const poll = async () => {
      await refresh();
      if (!cancelled) timer = setTimeout(poll, POLL_MS);
    };
    poll();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [refresh]);

  const send = useCallback(
    async (command) => {
      try {
        await command();
        setError(null);
      } catch (failure) {
        setError(failure.message);
      }
      refresh();
    },
    [refresh],
  );

  const chooseSpeed = useCallback(
    (next) => {
      setSpeed(next);
      try {
        localStorage.setItem(SPEED_KEY, JSON.stringify(next));
      } catch {
        // Storage blocked: the choice lasts this session.
      }
      if (status && ["starting", "playing", "paused"].includes(status.state)) send(() => setReplaySpeed(next));
    },
    [send, status],
  );

  return {
    status,
    error,
    speed,
    available: status !== null && status.state !== "unavailable",
    start: (symbol, start) => send(() => startReplay({ symbol, start, speed })),
    pause: () => send(pauseReplay),
    resume: () => send(resumeReplay),
    stop: () => send(stopReplay),
    setSpeed: chooseSpeed,
  };
}
