"""Erasure propagation: what "forget" means once cala's rows are in a warehouse.

The obligation
--------------
es-entity keeps personal data out of the durable event stream: a
`Forgettable<T>` field is stored in a `<table>_forgettable_payloads` row and
serialised as `null` into the event JSON, so `forget()` can delete the payload
and leave the events intact (`es-entity/src/forgettable.rs`). cala declares no
forgettable field, and none of its 17 tables is a payload table, so everything
an application writes into cala's `name`, `description`, `external_id` and
`metadata` is durable in cala and lands verbatim here: in the event JSON, in
the current-state tables, and a third time in the outbox payloads. An
append-only warehouse would keep it forever. This module is the counterpart
of `forget()` for the warehouse.

Three inputs, one mechanism
---------------------------
1. **Forget events observed upstream.** es-entity's convention is to stage a
   domain erasure event (an empty `Forgot {}`, `event_type = 'forgot'`) before
   calling `forget()`, so the erasure is on the stream and published through
   the outbox. Every run of `cala-extract` looks for those event types among
   the rows it just landed and files an erasure for the entity.
2. **Operator requests** (`cala-erase account <id> --reason DSAR-42`), because
   cala emits no forget event today and a data-subject request has to be
   honoured regardless.
3. **Cascades**: erasing an account with `--with-entries` also erases the
   free-text fields of every entry posted to it.

An erasure is *targeted deletion*: the personal fields of that one entity are
set to JSON `null` in every landed copy (events, state row, outbox payloads)
and nothing else changes. No entry, amount, id, sequence or timestamp is
touched, so the accounting controls hold after an erasure exactly as before
(the dbt test `assert_erased_entities_hold_no_personal_data` proves the
erasure, the reconciliation controls prove nothing else moved).

An erasure is a standing fact of the warehouse, not a one-off UPDATE: the
source still holds the data (cala never forgot it), and every extraction run
replaces the state tables and re-merges any entity the outbox announces
again. So after each run the extractor re-applies every logged erasure to the
rows that run landed. When that redacts something again, the log says so
(`source = 'reapply'`); that row is the audit trail of "upstream still holds
this and we scrubbed it again".

The log
-------
`cala_erasure_log` is append-only (dlt `append`), lives next to the raw
tables so dbt can read it (`stg_cala_erasures`, `fct_erasures`), and survives
`--full-refresh` because it belongs to its own dlt source. It records what was
erased (table, fields, row counts) and why (source, requester, reason), never
the values: no hash of the erased data is kept, since a salted hash of a name
is still a name to anyone with a dictionary.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Sequence

import dlt

from cala_extract.tables import OUTBOX

ERASURE_LOG = "cala_erasure_log"

# The event types that mean "the application forgot this entity"; es-entity's
# book uses `Forgot {}`, which serde writes as `forgot`.
FORGET_EVENT_TYPES: tuple[str, ...] = ("forgot",)

SOURCES = ("operator", "forget_event", "cascade", "reapply")


@dataclass(frozen=True)
class Kind:
    """One erasable entity type and where its personal fields live.

    `event_fields` are paths under the event JSON, `outbox_fields` keys under
    `payload[outbox_key]` (cala's outbox copies the entity values into the
    payload), `state_columns` plain columns of the current-state table.
    `shares_id_with` names kinds keyed by the same id: an account set's
    backing account has the set's id, so an erasure of either redacts both.
    """

    name: str
    stream: str
    event_fields: tuple[str, ...]
    outbox_key: str
    outbox_fields: tuple[str, ...]
    state_table: str | None = None
    state_columns: tuple[str, ...] = ()
    shares_id_with: tuple[str, ...] = ()

    @property
    def tables(self) -> tuple[str, ...]:
        return (self.stream, OUTBOX) + ((self.state_table,) if self.state_table else ())


_PERSONAL = ("name", "description", "external_id", "metadata")

KINDS: dict[str, Kind] = {
    k.name: k
    for k in (
        Kind(
            "account",
            "cala_account_events",
            tuple(f"values.{f}" for f in _PERSONAL),
            "account",
            _PERSONAL,
            "cala_accounts",
            ("name", "external_id"),
            shares_id_with=("account_set",),
        ),
        Kind(
            "account_set",
            "cala_account_set_events",
            tuple(f"values.{f}" for f in _PERSONAL),
            "account_set",
            _PERSONAL,
            "cala_account_sets",
            ("name", "external_id"),
            shares_id_with=("account",),
        ),
        Kind(
            "transaction",
            "cala_transaction_events",
            ("values.description", "values.external_id", "values.metadata"),
            "transaction",
            ("description", "external_id", "metadata"),
            "cala_transactions",
            ("external_id",),
        ),
        Kind(
            "entry",
            "cala_entry_events",
            ("values.description", "values.metadata"),
            "entry",
            ("description", "metadata"),
        ),
        Kind(
            "journal",
            "cala_journal_events",
            ("values.name", "values.description"),
            "journal",
            ("name", "description"),
            "cala_journals",
            ("name",),
        ),
    )
}


def kinds_for(kind_name: str) -> list[Kind]:
    """The kind plus the kinds that share its id, requested one first."""
    kind = KINDS[kind_name]
    return [kind, *(KINDS[k] for k in kind.shares_id_with)]


@dataclass
class LogRow:
    """One row of cala_erasure_log: one request applied to one table."""

    erasure_id: str
    applied_at: dt.datetime
    run_id: str
    source: str
    requested_by: str | None
    reason: str | None
    entity_kind: str
    entity_id: str
    cascade_of: str | None
    table_name: str
    fields: str  # JSON array of the field paths
    rows_matched: int
    rows_redacted: int
    log_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


LOG_COLUMNS: dict[str, dict[str, Any]] = {
    "log_id": {"data_type": "text", "nullable": False},
    "erasure_id": {"data_type": "text", "nullable": False},
    "applied_at": {"data_type": "timestamp", "timezone": False, "precision": 6},
    "run_id": {"data_type": "text"},
    "source": {"data_type": "text"},
    "requested_by": {"data_type": "text"},
    "reason": {"data_type": "text"},
    "entity_kind": {"data_type": "text"},
    "entity_id": {"data_type": "text"},
    "cascade_of": {"data_type": "text"},
    "table_name": {"data_type": "text"},
    "fields": {"data_type": "text"},
    "rows_matched": {"data_type": "bigint"},
    "rows_redacted": {"data_type": "bigint"},
}


@dlt.source(name="cala_warehouse", max_table_nesting=0)
def erasure_log_source(rows: Sequence[LogRow]):
    """The log as its own dlt source, so `refresh="drop_resources"` on the
    `cala` source (a --full-refresh) never drops it."""

    @dlt.resource(name=ERASURE_LOG, write_disposition="append", columns=LOG_COLUMNS)
    def log() -> Iterable[dict[str, Any]]:
        yield [r.as_dict() for r in rows]

    return log


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def validate_id(entity_id: str) -> str:
    try:
        return str(uuid.UUID(entity_id))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"entity id must be a UUID, got {entity_id!r}") from exc


def redact_json(doc: Any, paths: Iterable[str]) -> int:
    """Set each dotted path to null in place; return how many held a value.
    A missing key stays missing: the shape of the document is preserved."""
    n = 0
    for path in paths:
        keys = path.split(".")
        node = doc
        for key in keys[:-1]:
            node = node.get(key) if isinstance(node, dict) else None
        if isinstance(node, dict) and node.get(keys[-1]) is not None:
            node[keys[-1]] = None
            n += 1
    return n


# ------------------------------------------------------------------ the store


class Store:
    """The landed dataset through dlt's sql client, so the same statements run
    on DuckDB and BigQuery. Placeholders are `%s` on every destination (dlt
    rewrites them for DuckDB)."""

    def __init__(self, pipeline: dlt.Pipeline):
        self.pipeline = pipeline
        self._client: Any = None
        self._columns: dict[str, set[str]] = {}

    def __enter__(self) -> "Store":
        self._client = self.pipeline.sql_client().__enter__()
        return self

    def __exit__(self, *exc: Any) -> None:
        client, self._client = self._client, None
        client.__exit__(*exc)

    def table(self, name: str) -> str:
        return self._client.make_qualified_table_name(name)

    def query(self, sql: str, *args: Any) -> list[tuple[Any, ...]]:
        return [tuple(r) for r in (self._client.execute_sql(sql, *args) or [])]

    def execute(self, sql: str, *args: Any) -> None:
        self._client.execute_sql(sql, *args)

    def columns(self, name: str) -> set[str]:
        if name not in self._columns:
            with self.pipeline.destination_client() as client:
                exists, cols = client.get_storage_table(name)
            self._columns[name] = set(cols) if exists else set()
        return self._columns[name]

    def exists(self, name: str) -> bool:
        return bool(self.columns(name))


def _in(values: Sequence[Any]) -> str:
    return ", ".join(["%s"] * len(values))


def _load_filter(store: Store, table: str, load_ids: Sequence[str] | None) -> tuple[str, list[str]]:
    """Restrict to rows a given run landed (they carry its `_dlt_load_id`).
    Tables dbt seeded have no such column; then the filter is a no-op."""
    if not load_ids or "_dlt_load_id" not in store.columns(table):
        return "", []
    return f" and _dlt_load_id in ({_in(load_ids)})", list(load_ids)


@dataclass
class Touched:
    rows_matched: int = 0
    rows_redacted: int = 0


def apply_kind(
    store: Store, kind: Kind, ids: set[str], load_ids: Sequence[str] | None = None
) -> dict[str, dict[str, Touched]]:
    """Redact the kind's personal fields for `ids` in every landed copy.
    Returns entity_id -> table -> counts. With `load_ids`, only rows those
    loads wrote are visited (the re-apply after an extraction run)."""
    if not ids:
        return {}
    ordered = sorted(ids)
    out: dict[str, dict[str, Touched]] = {i: {} for i in ordered}

    def touched(entity_id: str, table: str) -> Touched:
        return out[entity_id].setdefault(table, Touched())

    # --- the event stream: rewrite the JSON of every event of the entity
    if store.exists(kind.stream):
        stream = store.table(kind.stream)
        extra, params = _load_filter(store, kind.stream, load_ids)
        rows = store.query(
            f"select id, sequence, event from {stream} where id in ({_in(ordered)}){extra}",
            *ordered, *params,
        )
        for entity_id, sequence, event in rows:
            t = touched(entity_id, kind.stream)
            t.rows_matched += 1
            doc = json.loads(event) if event else None
            if doc is None or redact_json(doc, kind.event_fields) == 0:
                continue
            store.execute(
                f"update {stream} set event = %s where id = %s and sequence = %s",
                json.dumps(doc), entity_id, int(sequence),
            )
            t.rows_redacted += 1

    # --- the current-state row: plain columns
    if kind.state_table and store.exists(kind.state_table):
        state = store.table(kind.state_table)
        cols = [c for c in kind.state_columns if c in store.columns(kind.state_table)]
        if cols:
            live = " or ".join(f"{c} is not null" for c in cols)
            for (entity_id,) in store.query(f"select id from {state} where id in ({_in(ordered)})", *ordered):
                touched(entity_id, kind.state_table).rows_matched += 1
            holders = [
                r[0]
                for r in store.query(f"select id from {state} where id in ({_in(ordered)}) and ({live})", *ordered)
            ]
            if holders:
                store.execute(
                    f"update {state} set {', '.join(f'{c} = null' for c in cols)} where id in ({_in(holders)})",
                    *holders,
                )
                for entity_id in holders:
                    touched(entity_id, kind.state_table).rows_redacted += 1

    # --- the outbox: the payload carries a copy of the entity's values
    if store.exists(OUTBOX):
        outbox = store.table(OUTBOX)
        extra, params = _load_filter(store, OUTBOX, load_ids)
        like, like_params = "", []
        if len(ordered) <= 100:
            # An id is a 36-char literal; LIKE narrows the scan cheaply.
            like = " and (" + " or ".join(["payload like %s"] * len(ordered)) + ")"
            like_params = [f"%{i}%" for i in ordered]
        rows = store.query(
            f"select sequence, payload from {outbox} where payload is not null{extra}{like}",
            *params, *like_params,
        )
        for sequence, payload in rows:
            doc = json.loads(payload)
            values = doc.get(kind.outbox_key) if isinstance(doc, dict) else None
            entity_id = values.get("id") if isinstance(values, dict) else None
            if entity_id not in out:
                continue
            t = touched(entity_id, OUTBOX)
            t.rows_matched += 1
            if redact_json(values, kind.outbox_fields):
                store.execute(f"update {outbox} set payload = %s where sequence = %s", json.dumps(doc), int(sequence))
                t.rows_redacted += 1
    return out


# ------------------------------------------------------------------ requests


@dataclass
class Erasure:
    """A request: erase one entity's personal fields, and say why."""

    entity_kind: str
    entity_id: str
    source: str = "operator"
    requested_by: str | None = None
    reason: str | None = None
    cascade_of: str | None = None
    erasure_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        if self.entity_kind not in KINDS:
            raise ValueError(f"unknown entity kind {self.entity_kind!r}; one of {sorted(KINDS)}")
        if self.source not in SOURCES:
            raise ValueError(f"unknown source {self.source!r}; one of {SOURCES}")
        self.entity_id = validate_id(self.entity_id)


def _rows_for(
    req: Erasure, applied: dict[str, Touched], kinds: list[Kind], run_id: str, when: dt.datetime, source: str
) -> list[LogRow]:
    fields_of = {}
    for k in kinds:
        fields_of[k.stream] = list(k.event_fields)
        fields_of[OUTBOX] = list(f"{k.outbox_key}.{f}" for f in k.outbox_fields)
        if k.state_table:
            fields_of[k.state_table] = list(k.state_columns)
    rows = []
    for table, t in applied.items():
        if source == "reapply" and t.rows_redacted == 0:
            continue
        if source != "reapply" and t.rows_matched == 0:
            continue
        rows.append(
            LogRow(
                erasure_id=req.erasure_id, applied_at=when, run_id=run_id, source=source,
                requested_by=req.requested_by, reason=req.reason, entity_kind=req.entity_kind,
                entity_id=req.entity_id, cascade_of=req.cascade_of, table_name=table,
                fields=json.dumps(fields_of.get(table, [])), rows_matched=t.rows_matched,
                rows_redacted=t.rows_redacted,
            )
        )
    if not rows and source != "reapply":
        # Nothing matched anywhere: still record that the request was received.
        k = kinds[0]
        rows.append(
            LogRow(
                erasure_id=req.erasure_id, applied_at=when, run_id=run_id, source=source,
                requested_by=req.requested_by, reason=req.reason, entity_kind=req.entity_kind,
                entity_id=req.entity_id, cascade_of=req.cascade_of, table_name=k.stream,
                fields=json.dumps(list(k.event_fields)), rows_matched=0, rows_redacted=0,
            )
        )
    return rows


def apply_requests(store: Store, requests: Sequence[Erasure], run_id: str) -> list[LogRow]:
    """Redact every copy for each request (all rows, no load filter) and
    return the log rows to append. Requests are batched per kind."""
    when = utcnow()
    by_kind: dict[str, set[str]] = {}
    for req in requests:
        for k in kinds_for(req.entity_kind):
            by_kind.setdefault(k.name, set()).add(req.entity_id)
    applied: dict[str, dict[str, Touched]] = {}
    for kind_name, ids in by_kind.items():
        for entity_id, per_table in apply_kind(store, KINDS[kind_name], ids).items():
            for table, t in per_table.items():
                agg = applied.setdefault(entity_id, {}).setdefault(table, Touched())
                agg.rows_matched += t.rows_matched
                agg.rows_redacted += t.rows_redacted
    rows: list[LogRow] = []
    for req in requests:
        rows.extend(_rows_for(req, applied.get(req.entity_id, {}), kinds_for(req.entity_kind), run_id, when, req.source))
    return rows


def entries_of_account(store: Store, account_id: str) -> list[str]:
    """Entry ids posted to the account, from cala's own entries table."""
    if not store.exists("cala_entries"):
        return []
    return [r[0] for r in store.query(f"select id from {store.table('cala_entries')} where account_id = %s", account_id)]


def write_log(pipeline: dlt.Pipeline, rows: Sequence[LogRow]) -> None:
    if rows:
        pipeline.run(erasure_log_source(list(rows)))


def read_log(store: Store) -> list[dict[str, Any]]:
    if not store.exists(ERASURE_LOG):
        return []
    cols = [c for c in LOG_COLUMNS]
    rows = store.query(f"select {', '.join(cols)} from {store.table(ERASURE_LOG)} order by applied_at, erasure_id, table_name")
    return [dict(zip(cols, r)) for r in rows]


def known_erasures(store: Store) -> list[Erasure]:
    """Every original request in the log (never the re-applies), one per
    entity, oldest first."""
    if not store.exists(ERASURE_LOG):
        return []
    rows = store.query(
        f"select entity_kind, entity_id, erasure_id, source from {store.table(ERASURE_LOG)} "
        f"where source <> 'reapply' "
        f"qualify row_number() over (partition by entity_kind, entity_id order by applied_at, erasure_id) = 1 "
        f"order by applied_at, entity_kind, entity_id"
    )
    return [Erasure(kind, entity_id, source=source, erasure_id=erasure_id) for kind, entity_id, erasure_id, source in rows]


# ------------------------------------------------------------ the operations


def erase(
    pipeline: dlt.Pipeline,
    entity_kind: str,
    entity_id: str,
    *,
    reason: str | None,
    requested_by: str | None = None,
    with_entries: bool = False,
    source: str = "operator",
    run_id: str | None = None,
) -> list[LogRow]:
    """Erase one entity now, in every landed copy, and log it.

    `with_entries` (accounts only) cascades to the entries posted to the
    account: each entry gets its own log rows with `cascade_of` set."""
    if with_entries and entity_kind != "account":
        raise ValueError("--with-entries applies to accounts only")
    run_id = run_id or f"cala-erase:{uuid.uuid4()}"
    request = Erasure(entity_kind, entity_id, source=source, requested_by=requested_by, reason=reason)
    with Store(pipeline) as store:
        requests = [request]
        if with_entries:
            requests += [
                Erasure("entry", eid, source="cascade", requested_by=requested_by, reason=reason, cascade_of=request.erasure_id)
                for eid in entries_of_account(store, request.entity_id)
            ]
        rows = apply_requests(store, requests, run_id)
    write_log(pipeline, rows)
    return rows


def reapply(store: Store, load_ids: Sequence[str] | None, run_id: str) -> list[LogRow]:
    """Re-redact every known erasure on the rows `load_ids` landed (or on
    every row when None). Logs only what actually had to be redacted again."""
    known = known_erasures(store)
    if not known:
        return []
    when = utcnow()
    by_kind: dict[str, set[str]] = {}
    for req in known:
        for k in kinds_for(req.entity_kind):
            by_kind.setdefault(k.name, set()).add(req.entity_id)
    applied: dict[str, dict[str, Touched]] = {}
    for kind_name, ids in by_kind.items():
        for entity_id, per_table in apply_kind(store, KINDS[kind_name], ids, load_ids).items():
            for table, t in per_table.items():
                agg = applied.setdefault(entity_id, {}).setdefault(table, Touched())
                agg.rows_matched += t.rows_matched
                agg.rows_redacted += t.rows_redacted
    rows: list[LogRow] = []
    for req in known:
        rows.extend(_rows_for(req, applied.get(req.entity_id, {}), kinds_for(req.entity_kind), run_id, when, "reapply"))
    return rows


def consume_forget_events(
    store: Store, load_ids: Sequence[str], run_id: str, event_types: Sequence[str] = FORGET_EVENT_TYPES
) -> list[LogRow]:
    """File an erasure for every entity whose stream, as landed by `load_ids`,
    carries a forget event that has not been honoured yet."""
    if not load_ids or not event_types:
        return []
    already: set[tuple[str, str]] = set()
    if store.exists(ERASURE_LOG):
        already = {
            (k, i)
            for k, i in store.query(
                f"select distinct entity_kind, entity_id from {store.table(ERASURE_LOG)} where source = 'forget_event'"
            )
        }
    requests: list[Erasure] = []
    for kind in KINDS.values():
        if not store.exists(kind.stream) or "_dlt_load_id" not in store.columns(kind.stream):
            continue
        rows = store.query(
            f"select id, event_type, sequence from {store.table(kind.stream)} "
            f"where _dlt_load_id in ({_in(load_ids)}) and event_type in ({_in(event_types)}) order by id, sequence",
            *load_ids, *event_types,
        )
        seen: set[str] = set()
        for entity_id, event_type, sequence in rows:
            if entity_id in seen or (kind.name, entity_id) in already:
                continue
            seen.add(entity_id)
            requests.append(
                Erasure(
                    kind.name, entity_id, source="forget_event", requested_by="upstream",
                    reason=f"{event_type} event at sequence {int(sequence)} on {kind.stream}",
                )
            )
    return apply_requests(store, requests, run_id) if requests else []


__all__ = [
    "ERASURE_LOG", "FORGET_EVENT_TYPES", "KINDS", "Erasure", "Kind", "LogRow", "Store",
    "apply_kind", "consume_forget_events", "erase", "erasure_log_source", "known_erasures",
    "read_log", "reapply", "redact_json", "write_log",
]
