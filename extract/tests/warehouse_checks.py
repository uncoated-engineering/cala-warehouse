"""Assertions over the landed DuckDB file, shared by the extractor tests."""

from __future__ import annotations

from pathlib import Path

import duckdb

from cala_extract.tables import ALL_TABLES

SEED_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "seed"


def raw_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return conn.execute(
        """
        select column_name, data_type from information_schema.columns
        where table_schema = 'raw' and table_name = ? and column_name not like '\\_dlt%' escape '\\'
        order by ordinal_position
        """,
        [table],
    ).fetchall()


def seed_relation(conn: duckdb.DuckDBPyConnection, table: str, typed_like: list[tuple[str, str]]) -> str:
    """SQL reading the committed seed with the same types the extractor landed:
    timestamps with a +00 offset become naive UTC, 't'/'f' become booleans."""
    casts = ", ".join(f'cast("{name}" as {data_type}) as "{name}"' for name, data_type in typed_like)
    path = SEED_DIR / f"{table}.csv"
    return (
        f"select {casts} from read_csv('{path}', header=true, all_varchar=true, "
        f"allow_quoted_nulls=false)"
    )


def assert_raw_equals_seeds(duckdb_path: Path, tables=ALL_TABLES) -> None:
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        conn.execute("set TimeZone = 'UTC'")
        for table in tables:
            cols = raw_columns(conn, table)
            names = ", ".join(f'"{n}"' for n, _ in cols)
            seed = seed_relation(conn, table, cols)
            landed = f'select {names} from raw."{table}"'
            missing = conn.execute(f"select count(*) from (({seed}) except ({landed}))").fetchone()[0]
            extra = conn.execute(f"select count(*) from (({landed}) except ({seed}))").fetchone()[0]
            assert (missing, extra) == (0, 0), f"{table}: {missing} seed rows missing, {extra} extra rows"
    finally:
        conn.close()


def count(duckdb_path: Path, table: str) -> int:
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        return conn.execute(f'select count(*) from raw."{table}"').fetchone()[0]
    finally:
        conn.close()
