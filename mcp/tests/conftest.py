"""Tests run against the DuckDB file and manifest that `make build` produces.

Nothing here builds the warehouse; CI runs `make build` first. If the
artefacts are missing the whole suite skips with a clear message rather than
failing on every test.
"""

from __future__ import annotations

import pytest

from cala_mcp.paths import DEFAULT_DUCKDB, DEFAULT_MANIFEST


@pytest.fixture(scope="session")
def manifest_path():
    if not DEFAULT_MANIFEST.exists():
        pytest.skip(f"{DEFAULT_MANIFEST} missing; run `make build` first")
    return DEFAULT_MANIFEST


@pytest.fixture(scope="session")
def duckdb_path():
    if not DEFAULT_DUCKDB.exists():
        pytest.skip(f"{DEFAULT_DUCKDB} missing; run `make build` first")
    return DEFAULT_DUCKDB


@pytest.fixture(scope="session")
def paths(manifest_path, duckdb_path):
    return {"manifest_path": manifest_path, "duckdb_path": duckdb_path}
