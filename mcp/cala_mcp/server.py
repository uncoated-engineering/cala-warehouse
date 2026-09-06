"""The MCP server: the tools in tools.py registered on an MCPServer over stdio.

Read-only by construction: the tools accept only the parameters below, there
is no SQL tool, and the DuckDB file is opened read_only. Paths come from
CALA_WAREHOUSE_MANIFEST / CALA_WAREHOUSE_DUCKDB or default to what
`make build` writes.
"""

from __future__ import annotations

import functools
from typing import Any, Callable

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from cala_mcp import tools
from cala_mcp.db import WarehouseMissing
from cala_mcp.manifest import ManifestError
from cala_mcp.paths import DEFAULT_DUCKDB, DEFAULT_MANIFEST

INSTRUCTIONS = """\
Read-only ledger semantics over the cala-warehouse dbt marts (DuckDB).
Balances live at (journal_id, account_id, currency, layer); layer is one of
settled | pending | encumbrance and must never be summed across. Amounts are
exact decimals returned as strings. Timestamps are naive UTC ISO-8601.
Start with describe_model to learn a model's columns, grain and lineage;
nothing here accepts SQL.
"""


def anticipated(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Turn the failures a caller can fix (unknown model or column, missing
    manifest or database, bad as_of) into ToolError so the message reaches
    the agent. Anything else is a crash and stays one."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except (ManifestError, WarehouseMissing, ValueError) as exc:
            raise ToolError(str(exc)) from exc

    return wrapper


def build_server() -> MCPServer:
    server = MCPServer(
        name="cala-warehouse",
        instructions=INSTRUCTIONS,
        version="0.1.0",
    )

    @server.tool(description=tools.describe_model.__doc__)
    @anticipated
    def describe_model(name: str) -> dict[str, Any]:
        return tools.describe_model(name, manifest_path=DEFAULT_MANIFEST, duckdb_path=DEFAULT_DUCKDB)

    @server.tool(description=tools.list_models.__doc__)
    @anticipated
    def list_models() -> list[dict[str, Any]]:
        return tools.list_models(manifest_path=DEFAULT_MANIFEST)

    @server.tool(description=tools.trial_balance.__doc__)
    @anticipated
    def trial_balance(journal_id: str, as_of: str | None = None) -> dict[str, Any]:
        return tools.trial_balance(
            journal_id, as_of, manifest_path=DEFAULT_MANIFEST, duckdb_path=DEFAULT_DUCKDB
        )

    @server.tool(description=tools.explain_account_balance.__doc__)
    @anticipated
    def explain_account_balance(
        account_id: str,
        currency: str,
        layer: str,
        journal_id: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        return tools.explain_account_balance(
            account_id, currency, layer, journal_id, limit,
            manifest_path=DEFAULT_MANIFEST, duckdb_path=DEFAULT_DUCKDB,
        )

    @server.tool(description=tools.reconcile.__doc__)
    @anticipated
    def reconcile(as_of: str | None = None, limit: int = 500) -> dict[str, Any]:
        return tools.reconcile(as_of, limit, manifest_path=DEFAULT_MANIFEST, duckdb_path=DEFAULT_DUCKDB)

    return server


def main() -> None:
    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
