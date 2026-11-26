"""
replay.py

The replay's controls (Phase 4a): what the chart's Replay button calls.
The replay itself is replay/replay_service.py, a process of its own; these
routes hand it commands and read its status through Redis
(redis_client.py's ReplayControlClient), so a backend reload never touches
a running replay.

- `GET /replay`: the service's status (see replay_service.py's docstring for
  its fields), or `{"state": "unavailable"}` if the service isn't running.
- `POST /replay/start` `{"symbol", "start", "speed", "source", "end"}`: start a
  replay (replacing any other). `speed` is a multiple of real time or
  "max"; `source` is "auto" (Bronze's live recording where it has the
  start, else the backfill's aggTrades), "trades" or "agg_trades".
- `POST /replay/pause`, `/replay/resume`, `/replay/stop`, and
  `POST /replay/speed` `{"speed"}`.

Every command answers 202: it's queued, and the status shows when it took
effect (a start takes seconds to minutes: it clears the last replay, copies
history and starts Spark). 503 if the service isn't running, rather than
queue a command nothing will read.

The replay's data is served by the same routes as live's, with
`?pipeline=replay` (routers/pipelines.py).

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Literal, Optional, Union

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from redis_client import ReplayControlClient

router = APIRouter(prefix="/replay")

# replay/clock.py's limits; the service checks again.
Speed = Union[Literal["max"], Annotated[float, Field(gt=0, le=1000)]]
Source = Literal["auto", "trades", "agg_trades"]
UNAVAILABLE = {"state": "unavailable"}


class StartReplay(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    start: datetime
    speed: Speed = 10
    source: Source = "auto"
    end: Optional[datetime] = None


class SetSpeed(BaseModel):
    speed: Speed


@router.get("")
async def get_status() -> dict:
    client = ReplayControlClient()
    try:
        return await client.status() or UNAVAILABLE
    finally:
        await client.close()


@router.post("/start", status_code=status.HTTP_202_ACCEPTED)
async def start(body: StartReplay) -> dict:
    if body.end is not None and _utc(body.end) <= _utc(body.start):
        raise HTTPException(status_code=422, detail="end must be after start")
    return await _send(
        {
            "command": "start",
            "symbol": body.symbol.upper(),
            "start": _utc(body.start).isoformat(),
            "end": _utc(body.end).isoformat() if body.end else None,
            "speed": body.speed,
            "source": body.source,
        }
    )


@router.post("/pause", status_code=status.HTTP_202_ACCEPTED)
async def pause() -> dict:
    return await _send({"command": "pause"})


@router.post("/resume", status_code=status.HTTP_202_ACCEPTED)
async def resume() -> dict:
    return await _send({"command": "resume"})


@router.post("/stop", status_code=status.HTTP_202_ACCEPTED)
async def stop() -> dict:
    return await _send({"command": "stop"})


@router.post("/speed", status_code=status.HTTP_202_ACCEPTED)
async def set_speed(body: SetSpeed) -> dict:
    return await _send({"command": "speed", "speed": body.speed})


async def _send(command: dict) -> dict:
    client = ReplayControlClient()
    try:
        if await client.status() is None:
            raise HTTPException(
                status_code=503, detail="the replay service isn't running (scripts/dev.sh starts it in its 'replay' window)"
            )
        await client.send(command)
    finally:
        await client.close()
    return {"queued": command["command"]}


def _utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment.astimezone(timezone.utc)
