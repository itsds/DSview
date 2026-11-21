"""Fixtures for the backend tests: the real FastAPI app, through Starlette's TestClient."""

from __future__ import annotations

import importlib.util
import socket
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_app():
    # Loaded by path: ingestion/ has a main.py too, and both folders are on sys.path.
    spec = importlib.util.spec_from_file_location("backend_main", REPO_ROOT / "backend" / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


@pytest.fixture(scope="session")
def app():
    return _load_app()


@pytest.fixture
def client(app, test_services):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def closed_port() -> int:
    """A local port with nothing listening on it: connections are refused at once."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
