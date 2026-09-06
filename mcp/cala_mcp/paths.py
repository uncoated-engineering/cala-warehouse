"""Where the dbt artefacts live.

Defaults are relative to the repo root (the parent of mcp/), which is where
`make build` writes them. Both can be overridden by environment variable so
the server can point at a warehouse built elsewhere.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_MANIFEST = Path(
    os.environ.get("CALA_WAREHOUSE_MANIFEST", REPO_ROOT / "dbt" / "target" / "manifest.json")
)
DEFAULT_DUCKDB = Path(
    os.environ.get("CALA_WAREHOUSE_DUCKDB", REPO_ROOT / "dbt" / "cala_warehouse.duckdb")
)
