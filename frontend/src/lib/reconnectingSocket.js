/**
 * reconnectingSocket.js
 *
 * A WebSocket that reconnects on its own — shared by every hook that
 * listens to the backend (hooks/useLiveCandles.js, hooks/useWhaleTrades.js),
 * so each keeps only what's specific to its data.
 *
 * Reconnects after any close it wasn't asked for: the backend restarting
 * (uvicorn --reload does that on every save), or closing with 1011 because
 * Redis or Kafka is unreachable. Waits back off exponentially, 1 s
 * doubling to 30 s. Only a socket that stayed open STABLE_MS resets the
 * wait to 1 s: a backend that accepts and then closes at once (1011 again)
 * keeps backing off, so a dependency that's down isn't retried every second.
 *
 * Plain JS rather than a hook, so it can be tested without React; the
 * WebSocket class and timers are injectable for the same reason.
 *
 * Author: @DS
 */

export const RETRY_INITIAL_MS = 1_000;
export const RETRY_MAX_MS = 30_000;
// Open at least this long before closing = a real connection, not a refusal.
export const STABLE_MS = 10_000;

/**
 * Opens `url` and keeps it open until the returned close() is called.
 *
 * onMessage(data) gets each message's data. onStatus(status) is told
 * "live" on every open and "reconnecting" whenever a retry is scheduled;
 * the status before the first open is the caller's to assume ("connecting").
 */
export function openReconnectingSocket(
  url,
  { onMessage, onStatus = () => {}, WebSocketImpl = globalThis.WebSocket, timers = globalThis, now = Date.now } = {},
) {
  let ws = null;
  let retryTimer = null;
  let retryMs = RETRY_INITIAL_MS;
  let openedAt = 0;
  let stopped = false;

  function connect() {
    ws = new WebSocketImpl(url);

    ws.onopen = () => {
      openedAt = now();
      onStatus("live");
    };

    ws.onmessage = (event) => {
      if (!stopped) onMessage(event.data);
    };

    // Browsers follow every failed or dropped socket's error event with a
    // close, so reconnecting is handled here alone.
    ws.onclose = () => {
      if (stopped) return;
      if (openedAt && now() - openedAt >= STABLE_MS) retryMs = RETRY_INITIAL_MS;
      openedAt = 0;
      onStatus("reconnecting");
      retryTimer = timers.setTimeout(connect, retryMs);
      retryMs = Math.min(retryMs * 2, RETRY_MAX_MS);
    };
  }

  connect();

  return function close() {
    stopped = true;
    timers.clearTimeout(retryTimer);
    ws.close();
  };
}
