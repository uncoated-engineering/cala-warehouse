"""What the extractor lands, and how each table is loaded.

Two load paths, one per shape in es-entity's contract:

* **streams** (`cala_<entity>_events`): append-only, `UNIQUE(id, sequence)`.
  Loaded incrementally, driven by the outbox (see source.py), and merged on
  `(id, sequence)` so replaying is idempotent.
* **state** (everything else): mutable current-state tables and cala's own
  balance projections. Replaced whole every run. A sequence watermark is
  wrong for these, so `replace` is the correct disposition, not a lazy one.

The outbox itself is a stream keyed on its global `sequence`.

Column lists are not hard-coded: `columns()` reads information_schema at run
time and applies one cast per type family, so a new column upstream lands
without a code change and the landed shapes match the committed seeds
(UUIDs and JSON as text, enums as text, timestamps as naive UTC).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import tuple_row

OUTBOX = "cala_persistent_outbox_events"


@dataclass(frozen=True)
class Stream:
    """An event stream and the outbox payloads that announce writes to it.

    `id_paths` maps an outbox payload `type` to the key path of the entity id
    inside that payload. An account set's backing account has no
    `account_created` of its own, so the account stream also listens to the
    `account_set_*` payloads (the set id *is* the backing account id).
    """

    table: str
    id_paths: dict[str, tuple[str, ...]]

    def ids_in(self, payload: dict[str, Any] | None) -> str | None:
        if not payload:
            return None
        path = self.id_paths.get(payload.get("type", ""))
        if path is None:
            return None
        node: Any = payload
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        return node if isinstance(node, str) else None


STREAMS: tuple[Stream, ...] = (
    Stream(
        "cala_journal_events",
        {"journal_created": ("journal", "id"), "journal_updated": ("journal", "id")},
    ),
    Stream(
        "cala_account_events",
        {
            "account_created": ("account", "id"),
            "account_updated": ("account", "id"),
            "account_set_created": ("account_set", "id"),
            "account_set_updated": ("account_set", "id"),
        },
    ),
    Stream(
        "cala_account_set_events",
        {
            "account_set_created": ("account_set", "id"),
            "account_set_updated": ("account_set", "id"),
        },
    ),
    Stream(
        "cala_transaction_events",
        {
            "transaction_created": ("transaction", "id"),
            "transaction_updated": ("transaction", "id"),
        },
    ),
    Stream("cala_entry_events", {"entry_created": ("entry", "id")}),
)

# Replaced every run. cala_tx_template_events is append-only but is loaded
# here on purpose: templates are immutable (the entity has one event variant)
# and tiny, and the fixtures contain a template with two `initialized` events
# and a single outbox row, so the outbox is not a complete change signal for
# it. Replacing 50 rows costs nothing; missing one would.
STATE: tuple[str, ...] = (
    "cala_journals",
    "cala_accounts",
    "cala_account_sets",
    "cala_account_set_member_accounts",
    "cala_account_set_member_account_sets",
    "cala_tx_templates",
    "cala_tx_template_events",
    "cala_transactions",
    "cala_entries",
    "cala_current_balances",
    "cala_balance_history",
)

ALL_TABLES: tuple[str, ...] = (OUTBOX, *(s.table for s in STREAMS), *STATE)


@dataclass(frozen=True)
class Columns:
    """The select list for one table and the dlt column hints that pin the
    landed types (every column is declared, so an all-NULL column such as
    `context` still gets created)."""

    names: tuple[str, ...]
    select_list: str
    hints: dict[str, dict[str, Any]]


def columns(conn: psycopg.Connection, table: str) -> Columns:
    with conn.cursor(row_factory=tuple_row) as cur:
        rows = cur.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = current_schema() and table_name = %s
            order by ordinal_position
            """,
            [table],
        ).fetchall()
    if not rows:
        raise LookupError(f"table {table!r} not found in the source database")

    exprs: list[str] = []
    hints: dict[str, dict[str, Any]] = {}
    for name, data_type in rows:
        quoted = f'"{name}"'
        if data_type == "timestamp with time zone":
            exprs.append(f"({quoted} at time zone 'utc') as {quoted}")
            hints[name] = {"data_type": "timestamp", "timezone": False, "precision": 6}
        elif data_type == "timestamp without time zone":
            exprs.append(quoted)
            hints[name] = {"data_type": "timestamp", "timezone": False, "precision": 6}
        elif data_type in ("uuid", "jsonb", "json", "USER-DEFINED"):
            # UUIDs for portability (BigQuery has none); JSON as text so
            # staging parses it through the cross-db macros; enums as text.
            exprs.append(f"{quoted}::text as {quoted}")
            hints[name] = {"data_type": "text"}
        elif data_type in ("character varying", "text", "character"):
            exprs.append(quoted)
            hints[name] = {"data_type": "text"}
        elif data_type in ("integer", "bigint", "smallint"):
            exprs.append(quoted)
            hints[name] = {"data_type": "bigint"}
        elif data_type == "boolean":
            exprs.append(quoted)
            hints[name] = {"data_type": "bool"}
        elif data_type == "date":
            exprs.append(quoted)
            hints[name] = {"data_type": "date"}
        elif data_type == "numeric":
            exprs.append(quoted)
            hints[name] = {"data_type": "decimal", "precision": 38, "scale": 18}
        else:
            raise TypeError(f"{table}.{name}: no cast rule for Postgres type {data_type!r}")
        hints[name]["name"] = name
    return Columns(tuple(r[0] for r in rows), ", ".join(exprs), hints)
