"""conftest.py — pytest fixtures and module-level mocking for Chicago 311 pipeline tests."""

import sys
from unittest.mock import MagicMock

import pytest

# ── Mock Prefect before it gets imported by any pipeline module ───────────────
# This prevents Prefect from trying to connect to a Prefect API server during
# test collection. Prefect is only used as a task orchestrator in this project;
# the actual logic in `_ingest_date_range` etc. is plain Python we want to unit test.

prefect_mock = MagicMock()
sys.modules["prefect"] = prefect_mock
sys.modules["prefect.task"] = MagicMock()
sys.modules["prefect.flow"] = MagicMock()


@pytest.fixture(scope="session", autouse=True)
def mock_prefect():
    """Apply prefect mocking at session scope so it covers all test modules."""
    yield
