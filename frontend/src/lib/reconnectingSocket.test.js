import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { RETRY_MAX_MS, openReconnectingSocket } from "./reconnectingSocket.js";

/** Stands in for the browser's WebSocket; the test drives its events. */
class FakeWebSocket {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.closed = false;
    FakeWebSocket.instances.push(this);
  }

  close() {
    this.closed = true;
  }

  // What the browser would do:
  open() {
    this.onopen?.();
  }
  receive(data) {
    this.onmessage?.({ data });
  }
  drop() {
    this.onclose?.();
  }
}

const latest = () => FakeWebSocket.instances[FakeWebSocket.instances.length - 1];

function open(handlers = {}) {
  const messages = [];
  const statuses = [];
  const close = openReconnectingSocket("ws://backend/ws/candles/BTCUSDT", {
    onMessage: (data) => messages.push(data),
    onStatus: (status) => statuses.push(status),
    WebSocketImpl: FakeWebSocket,
    ...handlers,
  });
  return { close, messages, statuses };
}

describe("openReconnectingSocket", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    FakeWebSocket.instances = [];
  });
  afterEach(() => vi.useRealTimers());

  it("connects, reports live and hands over each message", () => {
    const { messages, statuses } = open();
    expect(FakeWebSocket.instances).toHaveLength(1);
    expect(latest().url).toBe("ws://backend/ws/candles/BTCUSDT");

    latest().open();
    latest().receive('{"close":"100"}');
    expect(statuses).toEqual(["live"]);
    expect(messages).toEqual(['{"close":"100"}']);
  });

  it("reconnects after a drop, as after a backend restart, and carries on", () => {
    const { messages, statuses } = open();
    latest().open();
    latest().drop();
    expect(statuses).toEqual(["live", "reconnecting"]);

    vi.advanceTimersByTime(999);
    expect(FakeWebSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(FakeWebSocket.instances).toHaveLength(2);

    latest().open();
    latest().receive("after restart");
    expect(statuses).toEqual(["live", "reconnecting", "live"]);
    expect(messages).toEqual(["after restart"]);
  });

  it("backs off 1 s, 2 s, 4 s … up to 30 s while connections keep failing", () => {
    open();
    const waits = [];
    for (let attempt = 0; attempt < 7; attempt += 1) {
      latest().drop(); // refused straight away, or a 1011 right after opening
      const before = FakeWebSocket.instances.length;
      let waited = 0;
      while (FakeWebSocket.instances.length === before) {
        vi.advanceTimersByTime(1000);
        waited += 1000;
      }
      waits.push(waited);
    }
    expect(waits).toEqual([1000, 2000, 4000, 8000, 16000, RETRY_MAX_MS, RETRY_MAX_MS]);
  });

  it("starts the wait over after a connection that lasted", () => {
    open();
    latest().drop();
    vi.advanceTimersByTime(1000);
    latest().drop();
    vi.advanceTimersByTime(2000); // backing off: next wait would be 4 s

    latest().open();
    vi.advanceTimersByTime(10_000); // open long enough to count as real
    latest().drop();
    vi.advanceTimersByTime(1000);
    expect(FakeWebSocket.instances).toHaveLength(4);
  });

  it("close() closes the socket and stops for good", () => {
    const { close, messages, statuses } = open();
    const socket = latest();
    socket.open();

    close();
    expect(socket.closed).toBe(true);
    socket.receive("late");
    socket.drop(); // the close event the browser sends after close()
    vi.advanceTimersByTime(60_000);

    expect(messages).toEqual([]);
    expect(statuses).toEqual(["live"]);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("close() during a wait cancels the pending reconnect", () => {
    const { close } = open();
    latest().drop();
    close();
    vi.advanceTimersByTime(60_000);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });
});
