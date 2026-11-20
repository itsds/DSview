"""
fake_telegram.py

A stand-in for the Telegram Bot API, for the tests and for trying the bot
without messaging a phone. It answers the three methods
alerts/telegram_client.py calls (getMe, getChat, sendMessage) for one
token and one chat, records every message, and can be told to fail the
next sends (a 429 with retry_after, a 500, a 400) to exercise the retries.

In tests, as a fixture (`fake_telegram`): start(), then point the bot at
`.url` (DSVIEW_TELEGRAM_API_URL in a subprocess).

By hand, printing each message as it arrives:

    python tests/fake_telegram.py --port 8081
    # in alerts/'s shell:
    export DSVIEW_TELEGRAM_API_URL=http://127.0.0.1:8081
    export DSVIEW_TELEGRAM_BOT_TOKEN=test-token DSVIEW_TELEGRAM_CHAT_ID=42

Author: @DS
"""

from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

TOKEN = "test-token"
CHAT_ID = "42"


@dataclass
class FakeTelegram:
    token: str = TOKEN
    chat_id: str = CHAT_ID
    port: int = 0
    echo: bool = False
    messages: List[str] = field(default_factory=list)
    # (status, body) answers for the next sendMessage calls, used up in order.
    failures: List[Tuple[int, Dict[str, Any]]] = field(default_factory=list)
    send_attempts: int = 0
    _server: Optional[ThreadingHTTPServer] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def start(self) -> "FakeTelegram":
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 (http.server's naming)
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
                status, body = fake.answer(self.path, payload)
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args) -> None:
                pass  # quiet

        self._server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()

    def fail_next(self, status: int, description: str, retry_after: Optional[int] = None) -> None:
        body: Dict[str, Any] = {"ok": False, "error_code": status, "description": description}
        if retry_after is not None:
            body["parameters"] = {"retry_after": retry_after}
        with self._lock:
            self.failures.append((status, body))

    def answer(self, path: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        prefix, _, method = path.lstrip("/").partition("/")
        if prefix != f"bot{self.token}":
            # What Telegram answers for an unknown or revoked token.
            return 401, {"ok": False, "error_code": 401, "description": "Unauthorized"}
        if method == "getMe":
            return 200, {"ok": True, "result": {"id": 1, "is_bot": True, "username": "dsview_test_bot"}}
        if method == "getChat":
            if str(payload.get("chat_id")) != self.chat_id:
                return 400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}
            return 200, {"ok": True, "result": {"id": int(self.chat_id), "type": "private"}}
        if method == "sendMessage":
            with self._lock:
                self.send_attempts += 1
                if self.failures:
                    return self.failures.pop(0)
                if str(payload.get("chat_id")) != self.chat_id:
                    return 400, {"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}
                self.messages.append(payload["text"])
            if self.echo:
                print(f"--- message {len(self.messages)} ---\n{payload['text']}\n", flush=True)
            return 200, {"ok": True, "result": {"message_id": len(self.messages), "text": payload["text"]}}
        return 404, {"ok": False, "error_code": 404, "description": "Not Found"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fake Telegram Bot API that prints each message")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--token", default=TOKEN)
    parser.add_argument("--chat-id", default=CHAT_ID)
    args = parser.parse_args()
    fake = FakeTelegram(token=args.token, chat_id=args.chat_id, port=args.port, echo=True).start()
    print(f"Fake Telegram on {fake.url} (token {args.token!r}, chat {args.chat_id}); Ctrl+C to stop", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        fake.stop()
