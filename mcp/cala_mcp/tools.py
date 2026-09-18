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


# ---------------------------------------------------------------- reconcile

def _reconcile_ctes(manifest: Manifest, cutoff: dt.datetime | None) -> tuple[str, list[Any], dict[str, str]]:
    """CTE prefix shared by the leaf and set controls.

    warehouse_leaf: balances rebuilt from entries (fct_account_balances as
                    built, or fct_entries summed up to `cutoff`)
    cala_all:       cala's own balances for every account, sets included
                    (stg_cala_balances as built, or cala_balance_history at
                    the latest version written up to `cutoff`)
    accounts:       current account rows, to tell leaves from sets
    """
    balances = manifest.model("fct_account_balances").require(
        "journal_id", "account_id", "currency", "layer", "dr_balance", "cr_balance"
    )
    entries = manifest.model("fct_entries").require(
        "journal_id", "account_id", "currency", "layer", "debit_units", "credit_units", "recorded_at"
    )
    cala = manifest.model("stg_cala_balances").require(
        "journal_id", "account_id", "currency", "layer", "dr_balance", "cr_balance"
    )
    history = manifest.source("cala_balance_history").require(
        "journal_id", "account_id", "currency", "version", "values", "recorded_at"
    )
    accounts = manifest.model("dim_accounts").require("account_id", "is_account_set", "is_current")
    layers = cala.accepted_values("layer")
    if not layers:
        raise ValueError(f"{cala.name}.layer has no accepted_values test in the manifest; cannot unpivot history")

    params: list[Any] = []
    if cutoff is None:
        warehouse_leaf = (
            f"select {_select_list(balances, 'journal_id', 'account_id', 'currency', 'layer', 'dr_balance', 'cr_balance')} "
            f"from {balances.relation_name}"
        )
        cala_all = (
            f"select {_select_list(cala, 'journal_id', 'account_id', 'currency', 'layer', 'dr_balance', 'cr_balance')} "
            f"from {cala.relation_name}"
        )
        sources = {"warehouse": balances.name, "cala": cala.name}
    else:
        warehouse_leaf = f"""
            select
                {entries.col('journal_id')},
                {entries.col('account_id')},
                {entries.col('currency')},
                {entries.col('layer')},
                sum({entries.col('debit_units')})  as dr_balance,
                sum({entries.col('credit_units')}) as cr_balance
            from {entries.relation_name}
            where {entries.col('recorded_at')} <= ?
            group by 1, 2, 3, 4"""
        params.append(cutoff)
        # Same unpivot stg_cala_balances applies to cala_current_balances,
        # here on the history row that was current at the cutoff.
        unpivot = " union all ".join(
            f"""
            select
                {history.col('journal_id')},
                {history.col('account_id')},
                {history.col('currency')},
                '{layer}' as layer,
                cast(json_extract_string({history.col('values')}, '$.{layer}.dr_balance') as decimal(38, 18)) as dr_balance,
                cast(json_extract_string({history.col('values')}, '$.{layer}.cr_balance') as decimal(38, 18)) as cr_balance
            from history_at"""
            for layer in layers
        )
        cala_all = f"""
            with history_at as (
                select *
                from {history.relation_name}
                where {history.col('recorded_at')} <= ?
                qualify row_number() over (
                    partition by {history.col('journal_id')}, {history.col('account_id')}, {history.col('currency')}
                    order by {history.col('version')} desc
                ) = 1
            )
            {unpivot}"""
        params.append(cutoff)
        sources = {
            "warehouse": f"{entries.name} where recorded_at <= as_of",
            "cala": f"{history.name} at the latest version with recorded_at <= as_of",
        }

    ctes = f"""
    with warehouse_leaf as ({warehouse_leaf}),
    cala_all as ({cala_all}),
    accounts as (
        select {accounts.col('account_id')}, {accounts.col('is_account_set')}
        from {accounts.relation_name}
        where {accounts.col('is_current')}
    )"""
    return ctes, params, sources


_STATUS_CASE = """
        case
            when c.account_id is null then 'missing_in_cala'
            when w.account_id is null and (c.dr_balance <> 0 or c.cr_balance <> 0)
                                      then 'missing_in_warehouse'
            when w.account_id is null then 'ok_untouched_layer'
            when c.dr_balance <> w.dr_balance
              or c.cr_balance <> w.cr_balance
                                      then 'amount_mismatch'
            else 'ok'
        end                                                     as status"""


def _compared(cala_side: str, warehouse_side: str, key_label: str) -> str:
    return f"""
    compared as (
        select
            coalesce(c.journal_id, w.journal_id)                as journal_id,
            coalesce(c.account_id, w.account_id)                as {key_label},
            coalesce(c.currency, w.currency)                    as currency,
            coalesce(c.layer, w.layer)                          as layer,
            c.dr_balance                                        as cala_dr_balance,
            c.cr_balance                                        as cala_cr_balance,
            w.dr_balance                                        as warehouse_dr_balance,
            w.cr_balance                                        as warehouse_cr_balance,
            {_STATUS_CASE}
        from ({cala_side}) as c
        full outer join ({warehouse_side}) as w
            on  w.journal_id = c.journal_id
            and w.account_id = c.account_id
            and w.currency   = c.currency
            and w.layer      = c.layer
    )"""


def _run_control(wh: Warehouse, ctes: str, params: list[Any], compared: str, limit: int) -> dict[str, Any]:
    counts = wh.query(
        f"{ctes}, {compared} select status, count(*) as rows from compared group by 1 order by 1", params
    )
    mismatches = wh.query(
        f"{ctes}, {compared} select * from compared where status not in ('ok', 'ok_untouched_layer') "
        f"order by journal_id, 2, currency, layer limit ?",
        [*params, limit],
    )
    by_status = {c["status"]: c["rows"] for c in counts}
    mismatch_total = sum(n for s, n in by_status.items() if s not in ("ok", "ok_untouched_layer"))
    return {
        "rows_compared": sum(by_status.values()),
        "rows_by_status": by_status,
        "mismatch_count": mismatch_total,
        "mismatches_truncated": mismatch_total > len(mismatches),
        "mismatches": mismatches,
    }


def reconcile(
    as_of: str | None = None,
    limit: int = 500,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """Run the two balance reconciliation controls and return every mismatch.

    Leaf control: balances rebuilt from entries must equal cala's own
    persisted balances on the full (journal, account, currency, layer) grain,
    exact decimals, zero tolerance. Set control: cala's balance for an
    account set must equal our rollup of leaf balances through the
    membership closure, scoped to the set's journal. An empty `mismatches`
    list means green.

    With as_of, both sides are taken at that instant: entries by recorded_at
    and cala's side from cala_balance_history. The membership closure is
    always current.
    """
    manifest = _manifest(manifest_path)
    cutoff = parse_as_of(as_of)
    ctes, params, sources = _reconcile_ctes(manifest, cutoff)
    members = manifest.model("dim_account_set_members").require("account_set_id", "member_id", "member_kind")
    sets = manifest.model("dim_account_sets").require("account_set_id", "journal_id")
    members.check_value("member_kind", "account")

    leaf = _compared(
        cala_side="select c.* from cala_all as c join accounts as a on a.account_id = c.account_id where not a.is_account_set",
        warehouse_side="select * from warehouse_leaf",
        key_label="account_id",
    )
    rolled_up = f"""
        select
            b.journal_id,
            cl.account_set_id                                   as account_id,
            b.currency,
            b.layer,
            sum(b.dr_balance)                                   as dr_balance,
            sum(b.cr_balance)                                   as cr_balance
        from warehouse_leaf as b
        inner join (
            select distinct
                m.{members.col('account_set_id')},
                s.{sets.col('journal_id')},
                m.{members.col('member_id')}                     as account_id
            from {members.relation_name} as m
            inner join {sets.relation_name} as s
                on s.{sets.col('account_set_id')} = m.{members.col('account_set_id')}
            where m.{members.col('member_kind')} = 'account'
        ) as cl
            on  cl.account_id = b.account_id
            and cl.journal_id = b.journal_id
        group by 1, 2, 3, 4"""
    set_ = _compared(
        cala_side="select c.* from cala_all as c join accounts as a on a.account_id = c.account_id where a.is_account_set",
        warehouse_side=rolled_up,
        key_label="account_set_id",
    )

    with open_warehouse(duckdb_path) as wh:
        leaf_result = _run_control(wh, ctes, params, leaf, limit)
        set_result = _run_control(wh, ctes, params, set_, limit)

    controls = [
        {
            "name": "assert_balances_reconcile",
            "grain": ["journal_id", "account_id", "currency", "layer"],
            "compares": f"{sources['warehouse']} vs {sources['cala']}, leaf accounts only",
            **leaf_result,
        },
        {
            "name": "assert_account_set_balances_reconcile",
            "grain": ["journal_id", "account_set_id", "currency", "layer"],
            "compares": (
                f"{sources['warehouse']} rolled up through {members.name} (scoped to the set's journal) "
                f"vs {sources['cala']}, account sets only"
            ),
            **set_result,
        },
    ]
    mismatches = [
        {"control": c["name"], **m} for c in controls for m in c["mismatches"]
    ]
    for c in controls:
        del c["mismatches"]
    caveats = [
        "statuses: amount_mismatch (both sides have the grain, amounts differ), missing_in_cala "
        "(we have entries cala has no balance for), missing_in_warehouse (cala has a non-zero balance "
        "we have no entries for). A 0/0 layer on cala's side with no entries on ours is a match.",
    ]
    if cutoff is not None:
        caveats += [
            "as_of: the membership closure (dim_account_set_members) is as of now, not as of the cutoff.",
            "as_of: cala_balance_history.recorded_at is when cala wrote the balance (wall clock); "
            "fct_entries.recorded_at is the entry event's timestamp. Entries recorded with a backdated "
            "timestamp fall before the balance that includes them.",
        ]
    return {
        "as_of": cutoff.isoformat() if cutoff else None,
        "sources": sources,
        "controls": controls,
        "mismatches": mismatches,
        "mismatches_truncated": any(c["mismatches_truncated"] for c in controls),
        "is_reconciled": not mismatches and not any(c["mismatches_truncated"] for c in controls),
        "caveats": caveats,
    }


# ---------------------------------------------------------------- erasures

def erasures(
    entity_id: str | None = None,
    limit: int = 100,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """The erasure audit log: which entities had their personal fields
    redacted in the warehouse, on whose request, what it touched, and how
    many later extraction runs had to redact them again because the source
    still holds the value.

    One item per request from fct_erasures, newest first; `entity_id`
    narrows to one entity (an account and its account set share an id).
    Amounts, ids and sequences are never part of an erasure, so balances
    and reconciliation are unaffected; the dbt control
    assert_erased_entities_hold_no_personal_data proves each erasure holds.
    """
    manifest = _manifest(manifest_path)
    rel = manifest.model("fct_erasures")
    cols = ["erasure_id", "entity_kind", "entity_id", "erasure_source", "requested_by", "reason",
            "cascade_of", "run_id", "first_applied_at", "tables_touched", "rows_redacted_total",
            "reapply_count", "last_reapplied_at"]
    rel.require(*cols)
    params: list[Any] = []
    where = ""
    if entity_id:
        where = f" where {rel.col('entity_id')} = ?"
        params.append(entity_id)
    with open_warehouse(duckdb_path) as wh:
        items = wh.query(
            f"select {_select_list(rel, *cols)} from {rel.relation_name}{where} "
            f"order by {rel.col('first_applied_at')} desc, {rel.col('erasure_id')} limit ?",
            [*params, max(limit, 0)],
        )
        totals = wh.one(
            f"select count(*) as requests, count(distinct {rel.col('entity_id')}) as entities, "
            f"sum({rel.col('reapply_count')}) as reapplies from {rel.relation_name}{where}",
            params,
        ) or {}
    by_source: dict[str, int] = {}
    for it in items:
        by_source[it["erasure_source"]] = by_source.get(it["erasure_source"], 0) + 1
    result: dict[str, Any] = {
        "entity_id": entity_id,
        "grain": list(rel.grain),
        "requests_total": int(totals.get("requests") or 0),
        "entities_total": int(totals.get("entities") or 0),
        "reapplies_total": int(totals.get("reapplies") or 0),
        "shown": len(items),
        "by_source_shown": by_source,
        "erasures": items,
        "source": f"{rel.name}; proof: assert_erased_entities_hold_no_personal_data",
    }
    if not items:
        result["note"] = (
            f"no erasure on record for {entity_id!r}" if entity_id
            else "nothing has been erased in this warehouse (no cala-erase request, no forget event landed)"
        )
    return result


# ------------------------------------------------------------- quality KPIs

def quality_kpis(
    last_n: int = 10,
    *,
    manifest_path: Path | str = DEFAULT_MANIFEST,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
) -> dict[str, Any]:
    """The quality KPIs tracked over dbt runs: test pass rate (and whether
    every accounting control passed), source freshness lag, and row-count
    drift, newest run first.

    Returns the last_n rows of rpt_quality_kpis, the tests that failed or
    errored in the latest recorded run, and the models whose row count
    moved past the drift threshold in that run. The package's own hooks
    record every invocation after it ends, so the latest row describes the
    previous build; `make quality` refreshes the marts.
    """
    manifest = _manifest(manifest_path)
    kpis = manifest.model("rpt_quality_kpis")
    tests = manifest.model("fct_test_results")
    drift = manifest.model("fct_row_count_drift")

    kpi_cols = ["invocation_id", "run_started_at", "tests_run", "tests_passed", "tests_failed",
                "tests_errored", "pass_rate", "controls_run", "controls_passed", "all_controls_passed",
                "models_counted", "rows_total", "max_abs_drift_pct", "outlier_models",
                "entries_freshness_lag_s", "outbox_freshness_lag_s", "outbox_max_sequence"]
    kpis.require(*kpi_cols)
    test_cols = ["test_name", "test_kind", "tested_model", "status", "failures", "message"]
    tests.require("invocation_id", "passed", *test_cols)
    drift_cols = ["model_name", "row_count", "previous_row_count", "delta", "drift_pct"]
    drift.require("invocation_id", "is_outlier", *drift_cols)

    with open_warehouse(duckdb_path) as wh:
        runs = wh.query(
            f"select {_select_list(kpis, *kpi_cols)} from {kpis.relation_name} "
            f"order by {kpis.col('run_started_at')} desc, {kpis.col('invocation_id')} limit ?",
            [max(last_n, 1)],
        )
        failing: list[dict[str, Any]] = []
        outliers: list[dict[str, Any]] = []
        if runs:
            latest = runs[0]["invocation_id"]
            failing = wh.query(
                f"select {_select_list(tests, *test_cols)} from {tests.relation_name} "
                f"where {tests.col('invocation_id')} = ? and not {tests.col('passed')} "
                f"order by {tests.col('test_name')}",
                [latest],
            )
            outliers = wh.query(
                f"select {_select_list(drift, *drift_cols)} from {drift.relation_name} "
                f"where {drift.col('invocation_id')} = ? and {drift.col('is_outlier')} "
                f"order by {drift.col('model_name')}",
                [latest],
            )

    result: dict[str, Any] = {
        "source": f"{kpis.name}; failing tests from {tests.name}; outliers from {drift.name}",
        "grain": list(kpis.grain),
        "runs": runs,
        "latest_invocation_id": runs[0]["invocation_id"] if runs else None,
        "latest_failing_tests": failing,
        "latest_drift_outliers": outliers,
    }
    if not runs:
        result["note"] = (
            "no runs recorded yet: the hooks record an invocation after it ends, so the first "
            "rows appear after a second `make build` (or `make quality`)"
        )
    return result
