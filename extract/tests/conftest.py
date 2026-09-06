"""The extractor tests need a Postgres to be cala's database. Point
CALA_PG_URL at one (a throwaway: the fixtures DROP its public schema) or
the suite skips. CI uses a postgres service container; locally
`make fixture-pg` starts one in Docker.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cala_extract.fixture_db import load_fixtures
from cala_extract.pipeline import extract


@pytest.fixture(scope="session")
def pg_url() -> str:
    url = os.environ.get("CALA_PG_URL")
    if not url:
        pytest.skip("CALA_PG_URL not set; the extractor tests need a throwaway Postgres")
    return url


@pytest.fixture
def warehouse(tmp_path: Path) -> dict:
    """A scratch DuckDB file and dlt state directory, fresh per test."""
    return {
        "duckdb_path": tmp_path / "warehouse.duckdb",
        "pipelines_dir": tmp_path / "pipelines",
    }


@pytest.fixture
def run(pg_url, warehouse):
    """extract() bound to the test's source and scratch destination."""

    def _run(**kwargs):
        return extract(pg_url, **warehouse, **kwargs)

    return _run


@pytest.fixture
def full_fixtures(pg_url):
    return load_fixtures(pg_url)
