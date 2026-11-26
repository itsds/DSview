"""Fixtures for the replay tests: a live data root of the test's own, and Bronze written into it (bronze_fakes.py)."""

from __future__ import annotations

from pathlib import Path

import pytest

from bronze_fakes import SinkWriter


@pytest.fixture
def lake(tmp_path) -> Path:
    """A live data root of the test's own."""
    root = tmp_path / "data"
    root.mkdir()
    return root


@pytest.fixture
def sink(lake) -> SinkWriter:
    return SinkWriter(lake)
