/**
 * ids.js
 *
 * Unique ids for client-side objects: drawings and indicator instances.
 *
 * Author: @DS
 */

export function newId() {
  // randomUUID only exists in secure contexts (localhost counts, a LAN IP doesn't).
  return globalThis.crypto?.randomUUID?.() ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
