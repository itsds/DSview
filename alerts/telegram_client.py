"""
telegram_client.py

Telegram Bot API boundary: the one place in alerts/ that talks HTTP to
Telegram, the way streaming/alert_publisher.py is the one place that
produces to Kafka. telegram_bot.py hands it finished message text and
never sees a URL, a status code or the token.

Sending is patient by design, because an alert that waits beats one
that's lost (the alerts wait in Kafka meanwhile, uncommitted):

- **Rate limits.** Messages are spaced MIN_SEND_INTERVAL_SECONDS apart
  (3 s: Telegram's 20-per-minute limit for a group chat, which also covers
  its one-per-second guidance for a private chat). If Telegram still
  answers 429, the wait it names in `retry_after` is honoured and the
  message retried.
- **Outages.** A network error, a timeout or a 5xx is retried with
  exponential backoff, 1 s doubling to 60 s, for as long as it takes.
- **Bad requests.** Any other 4xx (a message Telegram rejects outright) is
  logged with Telegram's own description and dropped; retrying can't help.
- **Bad configuration.** A 401 or 404 means the token is wrong or
  revoked: TelegramConfigError, and the bot stops. check() runs the same
  test at startup, plus whether the bot can see the chat, so a typo fails
  at once instead of at the first whale.

A timeout can hide a message Telegram did deliver, so a retry after one
can occasionally send it twice.

The token is part of every Bot API URL, so nothing here ever logs a URL
or an httpx exception's text verbatim (both can carry it), and httpx's own
per-request INFO logging is switched off.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

import httpx

from config.settings import TELEGRAM_API_URL

logger = logging.getLogger(__name__)
# httpx logs every request URL at INFO — the token included.
logging.getLogger("httpx").setLevel(logging.WARNING)

MIN_SEND_INTERVAL_SECONDS = 3.0
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
REQUEST_TIMEOUT_SECONDS = 10.0


class TelegramConfigError(RuntimeError):
    """The token or chat id is wrong: nothing will send until it's fixed."""


class TelegramUnreachable(ConnectionError):
    """No answer from Telegram: a network error or a timeout."""


class TelegramClient:
    """Sends plain-text messages to one chat through the Bot API."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        api_url: str = TELEGRAM_API_URL,
        min_send_interval: float = MIN_SEND_INTERVAL_SECONDS,
        backoff_initial: float = BACKOFF_INITIAL_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        request_timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        if not token or not chat_id:
            raise TelegramConfigError("a bot token and a chat id are both required")
        self._token = token
        self._chat_id = chat_id
        self._base_url = f"{api_url.rstrip('/')}/bot{token}"
        self._min_send_interval = min_send_interval
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._request_timeout = request_timeout
        self._http: Optional[httpx.AsyncClient] = None
        self._last_sent_at = float("-inf")

    async def start(self) -> None:
        """Open the HTTP connection pool. Call once before anything else."""
        self._http = httpx.AsyncClient(base_url=self._base_url, timeout=self._request_timeout)

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()

    async def check(self) -> str:
        """
        Confirm the token works and the bot can see the chat; returns the bot's @username.

        Raises TelegramConfigError if either is wrong, and
        TelegramUnreachable if Telegram doesn't answer at all.
        """
        status, body = await self._call("getMe", {})
        if status in (401, 404) or not body.get("ok"):
            raise TelegramConfigError(f"Telegram rejected the bot token: {_describe(status, body)}")
        username = body["result"].get("username", "?")

        status, body = await self._call("getChat", {"chat_id": self._chat_id})
        if not body.get("ok"):
            raise TelegramConfigError(
                f"@{username} can't reach chat {self._chat_id}: {_describe(status, body)}. "
                "Message the bot first (or add it to the group), and check the chat id."
            )
        return username

    async def send_message(self, text: str) -> bool:
        """
        Send `text` to the chat; True once Telegram accepts it, False if it rejects it outright.

        Retries rate limits and outages until it succeeds (see the module
        docstring), so this can take a long time while Telegram is down.
        Raises TelegramConfigError if the token stops working.
        """
        backoff = self._backoff_initial
        while True:
            await self._pace()
            try:
                status, body = await self._call(
                    "sendMessage",
                    {"chat_id": self._chat_id, "text": text, "disable_web_page_preview": True},
                )
            except TelegramUnreachable as exc:
                logger.warning("Telegram unreachable (%s); retrying in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._backoff_max)
                continue

            if body.get("ok"):
                return True
            if status == 429:
                retry_after = float(body.get("parameters", {}).get("retry_after", backoff))
                logger.warning("Telegram rate limit hit; retrying in %.0fs as asked", retry_after)
                await asyncio.sleep(retry_after)
                continue
            if status >= 500:
                logger.warning("Telegram error (%s); retrying in %.0fs", _describe(status, body), backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._backoff_max)
                continue
            if status in (401, 404):
                raise TelegramConfigError(f"Telegram rejected the bot token: {_describe(status, body)}")
            logger.error("Telegram refused a message, dropping it (%s): %r", _describe(status, body), text)
            return False

    async def _pace(self) -> None:
        wait = self._last_sent_at + self._min_send_interval - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_sent_at = time.monotonic()

    async def _call(self, method: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """POST one Bot API method; (HTTP status, parsed body — {} if it isn't JSON)."""
        if self._http is None:
            raise RuntimeError("TelegramClient.start() must be called first")
        try:
            response = await self._http.post(f"/{method}", json=payload)
        except httpx.HTTPError as exc:
            # Its text can include the URL, and so the token: only the type goes on.
            raise TelegramUnreachable(f"{type(exc).__name__} calling {method}") from None
        try:
            body = response.json()
        except ValueError:
            body = {}
        return response.status_code, body if isinstance(body, dict) else {}


def _describe(status: int, body: Dict[str, Any]) -> str:
    """Telegram's own error text: "429 Too Many Requests: retry after 5"."""
    return f"{status} {body.get('description', 'no description')}"
