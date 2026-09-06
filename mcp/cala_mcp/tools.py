"""The tools, as plain functions.

server.py registers these with MCP; tests call them directly. Every function
takes the manifest and warehouse paths so tests can point them anywhere, and
every identifier in the SQL they build comes from the manifest.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cala_mcp.db import DEFAULT_DUCKDB, WarehouseMissing, open_warehouse
from cala_mcp.manifest import DEFAULT_MANIFEST, Manifest, Relation


def _manifest(path: Path | str) -> Manifest:
    return Manifest.load(path)


# ----------------------------------------------------------------- describe

def describe_model(
    name: str,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """Columns, descriptions, grain and lineage of a model, from the manifest.

    Data types come from the built DuckDB file when it exists; columns that
    exist physically but are not declared in the manifest are listed as
    `undocumented_columns` so the gap is visible instead of silently filled.
    """
    manifest = _manifest(manifest_path)
    rel = manifest.model(name)

    physical: dict[str, str] = {}
    warehouse_note: str | None = None
    try:
        with open_warehouse(duckdb_path) as wh:
            physical = {c["name"]: c["data_type"] for c in wh.columns(rel.schema, rel.identifier)}
            if not physical:
                warehouse_note = f"{rel.relation_name} is not built in {Path(duckdb_path).name}"
    except WarehouseMissing as exc:
        warehouse_note = str(exc)

    columns = [
        {
            "name": c.name,
            "description": c.description,
            "data_type": physical.get(c.name),
            "tests": list(c.tests),
        }
        for c in rel.columns.values()
    ]
    return {
        "name": rel.name,
        "resource_type": rel.resource_type,
        "materialized": rel.materialized,
        "relation": rel.relation_name,
        "description": rel.description,
        "grain": list(rel.grain),
        "columns": columns,
        "undocumented_columns": [c for c in physical if c not in rel.columns],
        "lineage": {
            "upstream": list(rel.upstream),
            "downstream": list(rel.downstream),
            "tests": list(rel.tests),
        },
        "source_file": rel.original_file_path,
        "manifest": {
            "path": str(manifest.path),
            "generated_at": manifest.metadata.get("generated_at"),
            "dbt_version": manifest.metadata.get("dbt_version"),
        },
        **({"warehouse_note": warehouse_note} if warehouse_note else {}),
    }


def list_models(*, manifest_path: Path | str = DEFAULT_MANIFEST) -> list[dict[str, Any]]:
    """Every model in the manifest with its grain and one-line description."""
    manifest = _manifest(manifest_path)
    out = []
    for name in manifest.names("model"):
        rel = manifest.model(name)
        out.append(
            {
                "name": rel.name,
                "grain": list(rel.grain),
                "description": rel.description.split("\n")[0],
                "column_count": len(rel.columns),
            }
        )
    return out
