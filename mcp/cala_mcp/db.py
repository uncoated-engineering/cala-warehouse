"""Read-only access to the DuckDB file dbt builds.

The connection is opened with read_only=True, so nothing this package does
can write. Rows come back as dicts of JSON-native values; DECIMAL(38,18)
becomes an exact decimal string, never a float.
"""

from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import duckdb

from cala_mcp.paths import DEFAULT_DUCKDB


class WarehouseMissing(Exception):
    pass


def decimal_str(value: Decimal) -> str:
    """Exact, human-readable decimal: '100', '0.5', '-12.000000000000000001'."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat(sep="T", timespec="microseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    return value


class Warehouse:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self._conn.execute(sql, params or [])
        names = [d[0] for d in cur.description]
        return [dict(zip(names, jsonable(row))) for row in cur.fetchall()]

    def one(self, sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        """Physical columns of a relation, from information_schema."""
        return self.query(
            """
            select column_name as name, data_type
            from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [schema, table],
        )


@contextmanager
def open_warehouse(path: Path | str = DEFAULT_DUCKDB) -> Iterator[Warehouse]:
    path = Path(path)
    if not path.exists():
        raise WarehouseMissing(
            f"DuckDB file not found at {path}. Run `make build` first."
        )
    conn = duckdb.connect(str(path), read_only=True)
    try:
        yield Warehouse(conn)
    finally:
        conn.close()
