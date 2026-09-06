"""MCP server over the cala-warehouse marts.

Read-only tools that expose ledger semantics (trial balance, balance
explanation, reconciliation, model documentation) to an agent. Every query is
grounded in dbt's manifest.json: model names, relation names and columns come
from there, never from hardcoded lists.
"""

from cala_mcp.paths import DEFAULT_DUCKDB, DEFAULT_MANIFEST, REPO_ROOT

__all__ = ["DEFAULT_DUCKDB", "DEFAULT_MANIFEST", "REPO_ROOT"]
