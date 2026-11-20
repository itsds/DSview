/**
 * config.js
 *
 * The frontend's one config module: where the backend lives. Every client
 * of backend/ (api/candles.js, hooks/useLiveCandles.js,
 * hooks/useWhaleTrades.js) imports these instead of keeping its own copy.
 *
 * Author: @DS
 */

// TODO(@DS): make these env-driven (Vite's import.meta.env.VITE_*) before
// this is ever deployed anywhere but localhost.
export const BACKEND_HTTP_URL = "http://localhost:8000";
export const BACKEND_WS_URL = "ws://localhost:8000";
