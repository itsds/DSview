"""Tests for config/settings.py's pipelines: live keeps its names, replay gets its own (Phase 4a)."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from config import settings
from config.settings import LIVE_PIPELINE, REPLAY_PIPELINE, REPO_ROOT, Pipeline, _with_search_path


def test_live_keeps_every_name_dsview_had_before_replay():
    live = LIVE_PIPELINE
    assert (live.trades_topic, live.depth_topic, live.whale_alerts_topic, live.candles_topic) == (
        "market.trades.raw",
        "market.depth.raw",
        "alerts.whales",
        "market.candles.1m",
    )
    assert live.consumer_group("dsview-whale-detector") == "dsview-whale-detector"
    assert live.redis_prefix == ""
    assert live.postgres_schema == "public"
    assert live.postgres_dsn == settings._DATABASE_DSN
    assert live.data_root == settings._LAKE_ROOT
    assert live.starting_offsets == "latest"


def test_replay_prefixes_every_name_and_starts_from_the_beginning():
    replay = REPLAY_PIPELINE
    assert (replay.trades_topic, replay.whale_alerts_topic, replay.candles_topic) == (
        "replay.market.trades.raw",
        "replay.alerts.whales",
        "replay.market.candles.1m",
    )
    assert replay.consumer_group("dsview-whale-detector") == "replay.dsview-whale-detector"
    assert replay.redis_prefix == "replay:"
    assert replay.postgres_schema == "replay"
    assert replay.data_root == os.path.join(settings._LAKE_ROOT, "replay")
    assert replay.starting_offsets == "earliest"


def test_an_unknown_pipeline_is_refused():
    with pytest.raises(ValueError, match="DSVIEW_PIPELINE must be one of"):
        Pipeline("staging")


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql://u:p@h:5432/db?options=-c%20search_path%3Dreplay"),
        ("postgresql://u:p@h/db?sslmode=disable", "postgresql://u:p@h/db?sslmode=disable&options=-c%20search_path%3Dreplay"),
        ("host=h dbname=db", "host=h dbname=db options='-c search_path=replay'"),
    ],
)
def test_a_pipeline_dsn_sets_only_its_schema_on_the_search_path(dsn, expected):
    assert _with_search_path(dsn, "replay") == expected


def test_a_dsn_with_options_of_its_own_is_refused():
    with pytest.raises(ValueError, match="already sets `options`"):
        _with_search_path("postgresql://h/db?options=-c%20x%3D1", "replay")


def test_the_module_names_follow_dsview_pipeline():
    script = (
        "import json; from config import settings as s; "
        "print(json.dumps([s.TRADES_TOPIC, s.WHALE_ALERTS_TOPIC, s.CANDLES_TOPIC, s.REDIS_PREFIX, s.DATA_ROOT, s.POSTGRES_DSN]))"
    )
    env = {**os.environ, "DSVIEW_PIPELINE": "replay", "PYTHONPATH": str(REPO_ROOT)}
    output = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True).stdout
    assert json.loads(output) == [
        REPLAY_PIPELINE.trades_topic,
        REPLAY_PIPELINE.whale_alerts_topic,
        REPLAY_PIPELINE.candles_topic,
        "replay:",
        REPLAY_PIPELINE.data_root,
        REPLAY_PIPELINE.postgres_dsn,
    ]


def test_a_misspelt_dsview_pipeline_stops_every_service_at_import():
    env = {**os.environ, "DSVIEW_PIPELINE": "replya", "PYTHONPATH": str(REPO_ROOT)}
    result = subprocess.run([sys.executable, "-c", "import config.settings"], env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert "unknown pipeline 'replya'" in result.stderr
