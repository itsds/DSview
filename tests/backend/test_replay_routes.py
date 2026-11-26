"""Tests for the replay's controls (backend/routers/replay.py): commands onto the service's list, its status back."""

from __future__ import annotations

import json

import pytest

from config.settings import REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY


@pytest.fixture
def controls(redis_client):
    """The replay's control keys, empty before and after (as if no service were running)."""
    redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)
    yield redis_client
    redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)


@pytest.fixture
def service_running(controls):
    controls.set(REPLAY_STATUS_KEY, json.dumps({"state": "idle"}), ex=60)
    return controls


def _commands(redis_client):
    return [json.loads(raw) for raw in reversed(redis_client.lrange(REPLAY_COMMANDS_KEY, 0, -1))]  # oldest first


def test_status_says_unavailable_while_no_service_runs(client, controls):
    assert client.get("/replay").json() == {"state": "unavailable"}


def test_status_is_the_services_own(client, controls):
    status = {"state": "playing", "symbol": "BTCUSDT", "clock": "2026-11-20T10:00:00+00:00", "speed": 10}
    controls.set(REPLAY_STATUS_KEY, json.dumps(status), ex=60)
    assert client.get("/replay").json() == status


def test_a_command_with_no_service_running_is_a_503_and_queues_nothing(client, controls):
    response = client.post("/replay/start", json={"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z"})
    assert response.status_code == 503
    assert _commands(controls) == []


def test_start_queues_a_normalized_command(client, service_running):
    response = client.post(
        "/replay/start", json={"symbol": "btcusdt", "start": "2026-11-20T15:30:00+05:30", "speed": "max", "source": "trades"}
    )
    assert response.status_code == 202
    assert _commands(service_running) == [
        {"command": "start", "symbol": "BTCUSDT", "start": "2026-11-20T10:00:00+00:00", "end": None, "speed": "max", "source": "trades"}
    ]


def test_start_defaults_to_ten_times_real_time_from_the_best_source(client, service_running):
    client.post("/replay/start", json={"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00"})  # naive: UTC
    [command] = _commands(service_running)
    assert (command["speed"], command["source"], command["start"]) == (10, "auto", "2026-11-20T10:00:00+00:00")


@pytest.mark.parametrize(
    "body",
    [
        {"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z", "speed": 0},
        {"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z", "speed": 5000},
        {"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z", "speed": "fast"},
        {"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z", "source": "klines"},
        {"symbol": "BTCUSDT", "start": "2026-11-20T10:00:00Z", "end": "2026-11-20T09:00:00Z"},
        {"start": "2026-11-20T10:00:00Z"},
    ],
)
def test_a_bad_start_is_a_422(client, service_running, body):
    assert client.post("/replay/start", json=body).status_code == 422
    assert _commands(service_running) == []


def test_pause_resume_speed_and_stop_are_queued_in_order(client, service_running):
    for path, body in (("pause", None), ("resume", None), ("speed", {"speed": 100}), ("stop", None)):
        assert client.post(f"/replay/{path}", json=body).status_code == 202
    assert _commands(service_running) == [
        {"command": "pause"},
        {"command": "resume"},
        {"command": "speed", "speed": 100},
        {"command": "stop"},
    ]
