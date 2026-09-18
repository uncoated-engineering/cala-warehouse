"""Defaults shared with the dbt project and the MCP server: the DuckDB file
dbt builds from, and where dlt keeps its local pipeline state."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DUCKDB = Path(
    os.environ.get("CALA_WAREHOUSE_DUCKDB", REPO_ROOT / "dbt" / "cala_warehouse.duckdb")
)
DEFAULT_PIPELINES_DIR = Path(
    os.environ.get("CALA_EXTRACT_PIPELINES_DIR", REPO_ROOT / ".dlt" / "pipelines")
)
DEFAULT_DATASET = "raw"
