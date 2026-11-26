"""
control.py

The replay service's side of its Redis controls (config/settings.py's
REPLAY_COMMANDS_KEY and REPLAY_STATUS_KEY); backend/redis_client.py's
ReplayControlClient is the other side.

- **Commands** are JSON objects on a list: the backend LPUSHes, the service
  BRPOPs, so each is handled once, in the order sent, and one sent while
  the service is busy waits for it rather than being missed (pub/sub would
  drop it).
- **Status** is one JSON object in a key the service rewrites every second
  with a STATUS_TTL_SECONDS expiry: a service that stopped, or crashed,
  reads as gone within seconds instead of as frozen in its last state.

Author: @DS
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import redis.asyncio as redis

from config.settings import REDIS_HOST, REDIS_PORT, REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY

STATUS_TTL_SECONDS = 5


class ReplayControl:
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        commands_key: str = REPLAY_COMMANDS_KEY,
        status_key: str = REPLAY_STATUS_KEY,
    ) -> None:
        self._client = redis.Redis(host=host, port=port, decode_responses=True)
        self._commands_key = commands_key
        self._status_key = status_key

    async def next_command(self, timeout: float) -> Optional[Dict[str, Any]]:
        """The oldest command waiting, waiting up to `timeout` s for one; None if none came. Skips anything that isn't a JSON object."""
        popped = await self._client.brpop([self._commands_key], timeout=timeout)
        if popped is None:
            return None
        try:
            command = json.loads(popped[1])
        except json.JSONDecodeError:
            return {"command": None, "raw": popped[1]}
        return command if isinstance(command, dict) else {"command": None, "raw": popped[1]}

    async def clear_commands(self) -> None:
        """Drop commands sent while no service was running: a stale "start" shouldn't fire on startup."""
        await self._client.delete(self._commands_key)

    async def publish_status(self, status: Dict[str, Any]) -> None:
        await self._client.set(self._status_key, json.dumps(status), ex=STATUS_TTL_SECONDS)

    async def clear_status(self) -> None:
        await self._client.delete(self._status_key)

    async def close(self) -> None:
        await self._client.aclose()
