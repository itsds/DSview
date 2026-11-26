"""
pipelines.py

The `pipeline` query parameter every route takes (Phase 4a): which
pipeline's data to serve, `live` (the default, so every URL from before
replay existed means what it meant) or `replay`. config/settings.py's
Pipeline says where each keeps its Redis keys, Postgres schema and Kafka
topics; routes hand that to their readers.

An unknown name is FastAPI's own validation error: 422 over REST, and a
close with 1008 over a WebSocket.

Author: @DS
"""

from __future__ import annotations

from typing import Literal

from fastapi import Query

from config.settings import LIVE, PIPELINE_NAMES, Pipeline

PipelineName = Literal[PIPELINE_NAMES]  # type: ignore[valid-type]  # Literal["live", "replay"]


def pipeline_query() -> PipelineName:
    """The parameter's declaration, shared so every route documents it the same way."""
    return Query(LIVE, description="Whose data: the live pipeline's, or the replay's")


def resolve(name: str) -> Pipeline:
    return Pipeline(name)
