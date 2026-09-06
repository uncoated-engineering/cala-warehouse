"""The tools, as plain functions.

server.py registers these with MCP; tests call them directly. Every function
takes the manifest and warehouse paths so tests can point them anywhere, and
every identifier in the SQL they build comes from the manifest.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from cala_mcp.db import DEFAULT_DUCKDB, Warehouse, WarehouseMissing, open_warehouse
from cala_mcp.manifest import DEFAULT_MANIFEST, Manifest, Relation


def _manifest(path: Path | str) -> Manifest:
    return Manifest.load(path)


def parse_as_of(value: str | None) -> dt.datetime | None:
    """ISO-8601 -> naive UTC datetime (the warehouse stores naive UTC timestamps)."""
    if value is None or value == "":
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"as_of must be ISO-8601 (e.g. 2026-01-31T00:00:00), got {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def _select_list(rel: Relation, *names: str, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"{prefix}{c}" for c in rel.cols(*names))


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
    for c in columns:
        allowed = rel.columns[c["name"]].accepted_values
        if allowed is not None:
            c["accepted_values"] = list(allowed)
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


# ------------------------------------------------------------ trial balance

def trial_balance(
    journal_id: str,
    as_of: str | None = None,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """Trial balance for one journal: per (currency, layer) total debits, total
    credits and their difference.

    Without as_of the rows come straight from rpt_trial_balance. With as_of
    they are recomputed from fct_entries restricted to recorded_at <= as_of,
    at the same grain and with the same column names.
    """
    manifest = _manifest(manifest_path)
    rpt = manifest.model("rpt_trial_balance")
    entries = manifest.model("fct_entries")
    cutoff = parse_as_of(as_of)

    row_cols = ["currency", "layer", "account_count", "entry_count", "total_debits",
                "total_credits", "difference", "is_balanced", "as_of"]
    rpt.require("journal_id", "journal_name", "journal_code", *row_cols)
    entries.require("journal_id", "currency", "layer", "account_id", "debit_units", "credit_units", "recorded_at")

    with open_warehouse(duckdb_path) as wh:
        journal = wh.one(
            f"select distinct {_select_list(rpt, 'journal_id', 'journal_name', 'journal_code')} "
            f"from {rpt.relation_name} where {rpt.col('journal_id')} = ?",
            [journal_id],
        )
        if cutoff is None:
            rows = wh.query(
                f"select {_select_list(rpt, *row_cols)} from {rpt.relation_name} "
                f"where {rpt.col('journal_id')} = ? order by {rpt.col('currency')}, {rpt.col('layer')}",
                [journal_id],
            )
            source = f"{rpt.name} (as built)"
        else:
            rows = wh.query(
                f"""
                select
                    {entries.col('currency')},
                    {entries.col('layer')},
                    count(distinct {entries.col('account_id')})                       as account_count,
                    count(*)                                                          as entry_count,
                    sum({entries.col('debit_units')})                                 as total_debits,
                    sum({entries.col('credit_units')})                                as total_credits,
                    sum({entries.col('debit_units')}) - sum({entries.col('credit_units')}) as difference,
                    sum({entries.col('debit_units')}) = sum({entries.col('credit_units')}) as is_balanced,
                    max({entries.col('recorded_at')})                                 as as_of
                from {entries.relation_name}
                where {entries.col('journal_id')} = ? and {entries.col('recorded_at')} <= ?
                group by 1, 2
                order by 1, 2
                """,
                [journal_id, cutoff],
            )
            source = f"recomputed from {entries.name} where recorded_at <= as_of"

    result: dict[str, Any] = {
        "journal_id": journal_id,
        "journal_name": journal["journal_name"] if journal else None,
        "journal_code": journal["journal_code"] if journal else None,
        "as_of": cutoff.isoformat() if cutoff else None,
        "source": source,
        "grain": [c for c in rpt.grain if c != "journal_id"],
        "rows": rows,
        "is_balanced": all(r["is_balanced"] for r in rows) if rows else None,
    }
    if journal is None:
        result["note"] = f"journal {journal_id!r} has no rows in {rpt.name}; unknown journal or no entries"
    return result


# ------------------------------------------------- explain account balance

def explain_account_balance(
    account_id: str,
    currency: str,
    layer: str,
    journal_id: str | None = None,
    limit: int = 200,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """The balance on (account, currency, layer) and the entry chain that
    produced it, in posting order with running totals.

    An account may hold the same (currency, layer) in several journals; each
    journal's balance is a separate item unless journal_id narrows it. cala's
    own persisted figure for the grain is included for comparison. `limit`
    caps the entries returned per balance (the most recent ones); running
    totals are still computed over the whole chain.
    """
    manifest = _manifest(manifest_path)
    balances = manifest.model("fct_account_balances")
    entries = manifest.model("fct_entries")
    accounts = manifest.model("dim_accounts")
    cala = manifest.model("stg_cala_balances")

    balances.require("journal_id", "account_id", "currency", "layer", "dr_balance", "cr_balance",
                     "balance", "normal_balance_type", "entry_count", "latest_entry_id",
                     "first_entry_at", "last_entry_at")
    balances.check_value("layer", layer)
    entries.require("entry_id", "transaction_id", "journal_id", "account_id", "currency", "layer",
                    "direction", "units", "debit_units", "credit_units", "entry_type",
                    "entry_sequence", "description", "effective_date", "recorded_at")
    accounts.require("account_id", "name", "code", "normal_balance_type", "is_account_set", "is_current")
    cala.require("journal_id", "account_id", "currency", "layer", "dr_balance", "cr_balance", "latest_entry_id")

    grain_filter = (
        f"{{a}}.{balances.col('account_id')} = ? and {{a}}.{balances.col('currency')} = ? "
        f"and {{a}}.{balances.col('layer')} = ?"
    )
    params: list[Any] = [account_id, currency, layer]
    journal_filter = ""
    if journal_id:
        journal_filter = f" and {{a}}.{balances.col('journal_id')} = ?"
        params.append(journal_id)

    with open_warehouse(duckdb_path) as wh:
        account = wh.one(
            f"select {_select_list(accounts, 'account_id', 'name', 'code', 'normal_balance_type', 'is_account_set')} "
            f"from {accounts.relation_name} where {accounts.col('account_id')} = ? and {accounts.col('is_current')}",
            [account_id],
        )
        balance_rows = wh.query(
            f"select {_select_list(balances, 'journal_id', 'dr_balance', 'cr_balance', 'balance', 'normal_balance_type', 'entry_count', 'latest_entry_id', 'first_entry_at', 'last_entry_at', alias='b')} "
            f"from {balances.relation_name} as b where {grain_filter.format(a='b')}{journal_filter.format(a='b')} "
            f"order by b.{balances.col('journal_id')}",
            params,
        )
        cala_rows = {
            r["journal_id"]: r
            for r in wh.query(
                f"select {_select_list(cala, 'journal_id', 'dr_balance', 'cr_balance', 'latest_entry_id', alias='c')} "
                f"from {cala.relation_name} as c where {grain_filter.format(a='c')}{journal_filter.format(a='c')}",
                params,
            )
        }

        items = []
        for b in balance_rows:
            signed = (
                f"sum(e.{entries.col('debit_units')}) over w - sum(e.{entries.col('credit_units')}) over w"
                if b["normal_balance_type"] == "debit"
                else f"sum(e.{entries.col('credit_units')}) over w - sum(e.{entries.col('debit_units')}) over w"
            )
            chain = wh.query(
                f"""
                select
                    row_number() over w                                         as position,
                    {_select_list(entries, 'entry_id', 'transaction_id', 'recorded_at', 'effective_date',
                                  'entry_sequence', 'entry_type', 'direction', 'units', 'description', alias='e')},
                    sum(e.{entries.col('debit_units')}) over w                    as running_dr,
                    sum(e.{entries.col('credit_units')}) over w                   as running_cr,
                    {signed}                                                    as running_balance
                from {entries.relation_name} as e
                where e.{entries.col('journal_id')} = ? and e.{entries.col('account_id')} = ?
                  and e.{entries.col('currency')} = ? and e.{entries.col('layer')} = ?
                window w as (
                    order by e.{entries.col('recorded_at')}, e.{entries.col('entry_sequence')}, e.{entries.col('entry_id')}
                    rows between unbounded preceding and current row
                )
                qualify row_number() over w > (count(*) over ()) - ?
                order by position
                """,
                [b["journal_id"], account_id, currency, layer, max(limit, 0)],
            )
            last = chain[-1] if chain else None
            cala_row = cala_rows.get(b["journal_id"])
            items.append(
                {
                    "journal_id": b["journal_id"],
                    "dr_balance": b["dr_balance"],
                    "cr_balance": b["cr_balance"],
                    "balance": b["balance"],
                    "balance_sign": (
                        f"{'dr - cr' if b['normal_balance_type'] == 'debit' else 'cr - dr'} "
                        f"({b['normal_balance_type']}-normal account)"
                    ),
                    "entry_count": b["entry_count"],
                    "first_entry_at": b["first_entry_at"],
                    "last_entry_at": b["last_entry_at"],
                    "latest_entry_id": b["latest_entry_id"],
                    "chain_reconciles": bool(
                        last and last["running_dr"] == b["dr_balance"] and last["running_cr"] == b["cr_balance"]
                    ),
                    "entries_shown": len(chain),
                    "entries_truncated": len(chain) < int(b["entry_count"]),
                    "entries": chain,
                    "cala": (
                        {
                            "dr_balance": cala_row["dr_balance"],
                            "cr_balance": cala_row["cr_balance"],
                            "latest_entry_id": cala_row["latest_entry_id"],
                            "matches": cala_row["dr_balance"] == b["dr_balance"]
                            and cala_row["cr_balance"] == b["cr_balance"],
                        }
                        if cala_row
                        else None
                    ),
                }
            )

    result: dict[str, Any] = {
        "account_id": account_id,
        "currency": currency,
        "layer": layer,
        "journal_id": journal_id,
        "account": account,
        "grain": list(balances.grain),
        "balances": items,
        "source": f"{balances.name}; chain from {entries.name}; cala figure from {cala.name}",
    }
    if not items:
        if account is None:
            result["note"] = f"account {account_id!r} is not in {accounts.name}"
        elif account["is_account_set"]:
            result["note"] = (
                "this account backs an account set; no entry posts to it and its balance is a "
                "rollup of its members (see reconcile and dim_account_set_members)"
            )
        else:
            result["note"] = f"no entries on ({account_id}, {currency}, {layer})"
            if cala_rows:
                result["cala"] = list(cala_rows.values())
    return result
