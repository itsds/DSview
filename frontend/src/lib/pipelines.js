/**
 * pipelines.js
 *
 * Which pipeline's data the chart shows: live's, or a replay's (Phase 4a).
 * The backend serves both through the same routes, the replay's with
 * `?pipeline=replay` (backend/routers/pipelines.py); config/settings.py's
 * PIPELINE_NAMES must match these names.
 *
 * A *feed* is what the chart and its hooks key on: the pipeline, plus which
 * replay run. Every run starts the replay's data afresh, so a new run must
 * remount the chart and refetch, even though the pipeline's name is the same.
 *
 * Author: @DS
 */

export const LIVE = "live";
export const REPLAY = "replay";

export const LIVE_FEED = Object.freeze({ pipeline: LIVE, key: LIVE });

export function replayFeed(run) {
  return Object.freeze({ pipeline: REPLAY, key: `${REPLAY}#${run}` });
}

/** `params` (URLSearchParams) with the pipeline added, unless it's live's: live's URLs stay as they always were. */
export function withPipeline(params, pipeline) {
  if (pipeline && pipeline !== LIVE) params.set("pipeline", pipeline);
  return params;
}

// Replay states in which the chart shows the replay (replay/replay_service.py's).
const SHOWN_STATES = new Set(["starting", "playing", "paused", "finished"]);

/**
 * The feed the chart of `symbol` should show, given the replay service's
 * status: the replay's once its history is in place (`seeded`; before that
 * it has nothing to show), live's otherwise, including when the replay is
 * of another symbol or failed.
 */
export function feedFor(status, symbol) {
  if (status && SHOWN_STATES.has(status.state) && status.seeded && status.symbol === symbol) {
    return replayFeed(status.run);
  }
  return LIVE_FEED;
}

/** True while a replay is under way (starting, playing, paused or finished): the replay bar shows. */
export function replayActive(status) {
  return Boolean(status && SHOWN_STATES.has(status.state));
}
