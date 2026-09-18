"""The dlt source: one consistent snapshot of cala's Postgres, with the outbox
as the change log.

Why the outbox and not `recorded_at`
------------------------------------
es-entity writes `recorded_at` as `COALESCE($caller_supplied, NOW())`: the
application may backdate it (cala's own tests post entries dated a year
earlier than the day they ran) and `NOW()` is transaction *start* time, so
commit order is not `recorded_at` order either. No column on the `*_events`
tables is monotonic, and a time cursor would silently miss rows.

The outbox's `sequence` is the one monotonic cursor in the schema: a
`BIGSERIAL` primary key, `CACHE 1`, and obix (cala's outbox library) keeps
the log gap-free by inserting `payload NULL` placeholder rows for sequences
whose transaction rolled back. Every write cala makes to an entity is
announced on it in the same transaction. So:

1. Read outbox rows with `sequence > watermark` in order.
2. From their payloads collect the entity ids each stream needs, and fetch
   those entities' `*_events` rows by key. Merge on `(id, sequence)`.
3. Advance the watermark only across a *contiguous* run of sequences. A
   missing sequence below the highest one seen is either an in-flight
   transaction (wait) or an abandoned one (skip); which of the two is
   decided the way obix decides it, see `resolve_watermark`.

All reads happen in a single REPEATABLE READ, READ ONLY transaction, so the
streams and the replaced state tables describe the same instant and the
reconciliation controls hold after every run, not only when cala is idle.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterator

import dlt
import psycopg
from psycopg.rows import dict_row

from cala_extract.tables import ALL_TABLES, OUTBOX, STATE, STREAMS, Columns, Stream, columns

# A gap wider than this is not an obix-shaped gap (each rolled-back
# transaction burns exactly one sequence); stop enumerating and stall.
GAP_ENUMERATION_LIMIT = 10_000


@dataclass
class Snapshot:
    """A REPEATABLE READ, READ ONLY transaction on the source.

    `marker` is an xid taken by a separate auto-commit statement right after
    the snapshot was established, the way obix's `abandonment_marker` does
    it. Xids are handed out in order, so every transaction that had begun
    writing before the snapshot holds a smaller xid than the marker, and the
    marker's own transaction is over immediately, so it never holds the
    horizon back itself. (The snapshot's xmax is not usable for this: it is
    "latest completed xid + 1", and a running transaction can sit at or
    above it.)
    """

    conn: psycopg.Connection
    marker: int
    _cursors: int = 0

    @classmethod
    def open(cls, url: str) -> "Snapshot":
        conn = psycopg.connect(url, autocommit=False, row_factory=dict_row)
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        # The first statement establishes the snapshot. Then the marker, on
        # its own connection so its transaction ends at once.
        conn.execute("select pg_current_snapshot()")
        with psycopg.connect(url, autocommit=True) as marker_conn:
            marker = marker_conn.execute("select pg_current_xact_id()::text").fetchone()[0]
        return cls(conn=conn, marker=int(marker))

    def close(self) -> None:
        try:
            self.conn.rollback()
        finally:
            self.conn.close()

    def count(self, table: str) -> int:
        return self.conn.execute(f"select count(*) as n from {table}").fetchone()["n"]

    def pages(self, sql: str, params: list[Any], page_size: int) -> Iterator[list[dict[str, Any]]]:
        """Stream a query through a server-side cursor, one page at a time."""
        self._cursors += 1
        with self.conn.cursor(name=f"cala_extract_{self._cursors}") as cur:
            cur.itersize = page_size
            cur.execute(sql, params)
            while rows := cur.fetchmany(page_size):
                yield rows


@dataclass
class RunState:
    """What one run learned; filled in during extraction, reported after."""

    watermark_before: int = 0
    watermark_after: int = 0
    highest_seen: int = 0
    # The sequence the watermark stopped short of, if it could not advance
    # to `highest_seen`: an in-flight transaction, or a late commit that the
    # next run will load.
    stalled_at: int | None = None
    # Sequences proven abandoned and skipped over.
    abandoned: list[int] = field(default_factory=list)
    # outbox page (keyed by its first sequence) -> stream table -> entity ids
    touched: dict[int, dict[str, set[str]]] = field(default_factory=dict)
    source_counts: dict[str, int] = field(default_factory=dict)


def resolve_watermark(
    source_url: str, marker: int, highest_seen: int, missing: list[int]
) -> tuple[int, int | None, list[int]]:
    """Decide how far the watermark may advance given the sequences that were
    missing below `highest_seen` in the snapshot.

    Returns (new_watermark, stalled_at, abandoned).

    The argument is obix's own (`abandonment_proof_passed`): every sequence
    below one that was visible in the snapshot was allocated before the
    snapshot, by a transaction with `xid < marker`. Once the xmin horizon has
    passed the marker (`pg_snapshot_xmin(pg_current_snapshot()) > marker` in
    a fresh transaction), every such transaction has ended. A sequence that
    is *still* absent then was never committed: it is abandoned and safe to
    skip. One that appeared meanwhile was a late commit; the watermark stops
    before it so the next run loads it. If the horizon has not passed, a
    writer may still own the gap and the watermark stops at the first hole.
    """
    if not missing:
        return highest_seen, None, []
    first = missing[0]
    with psycopg.connect(source_url, autocommit=True) as conn:
        passed = conn.execute(
            "select pg_snapshot_xmin(pg_current_snapshot()) > %s::text::xid8 as passed",
            [str(marker)],
        ).fetchone()[0]
        if not passed:
            return first - 1, first, []
        present = {
            r[0]
            for r in conn.execute(
                f"select sequence from {OUTBOX} where sequence = any(%s::bigint[])", [missing]
            ).fetchall()
        }
    abandoned: list[int] = []
    for seq in missing:
        if seq in present:
            return seq - 1, seq, abandoned
        abandoned.append(seq)
    return highest_seen, None, abandoned


@dlt.source(name="cala", max_table_nesting=0)
def cala_source(
    snapshot: Snapshot,
    run: RunState,
    *,
    source_url: str,
    watermark: int,
    page_size: int = 5000,
    verify: bool = False,
):
    conn = snapshot.conn
    if verify:
        run.source_counts = {t: snapshot.count(t) for t in ALL_TABLES}

    outbox_cols = columns(conn, OUTBOX)
    stream_cols = {s.table: columns(conn, s.table) for s in STREAMS}

    @dlt.resource(
        name=OUTBOX,
        write_disposition="merge",
        primary_key="sequence",
        columns=outbox_cols.hints,
    )
    def outbox() -> Iterator[list[dict[str, Any]]]:
        state = dlt.current.resource_state()
        run.watermark_before = watermark
        expected = watermark + 1
        highest = watermark
        missing: list[int] = []
        sql = f"select {outbox_cols.select_list} from {OUTBOX} where sequence > %s order by sequence"
        for page in snapshot.pages(sql, [watermark], page_size):
            touched: dict[str, set[str]] = {s.table: set() for s in STREAMS}
            for row in page:
                seq = int(row["sequence"])
                if seq > expected and len(missing) < GAP_ENUMERATION_LIMIT:
                    missing.extend(range(expected, min(seq, expected + GAP_ENUMERATION_LIMIT - len(missing))))
                expected = seq + 1
                highest = seq
                payload = json.loads(row["payload"]) if row["payload"] else None
                for stream in STREAMS:
                    entity_id = stream.ids_in(payload)
                    if entity_id:
                        touched[stream.table].add(entity_id)
            run.touched[int(page[0]["sequence"])] = touched
            yield page

        run.highest_seen = highest
        run.watermark_after, run.stalled_at, run.abandoned = resolve_watermark(
            source_url, snapshot.marker, highest, missing
        )
        state["watermark"] = run.watermark_after
        state["highest_seen"] = highest

    def scan(table: str, cols: Columns, order_by: str) -> Iterator[list[dict[str, Any]]]:
        yield from snapshot.pages(
            f"select {cols.select_list} from {table} order by {order_by}", [], page_size
        )

    def keyed(stream: Stream, cols: Columns):
        seen: set[str] = set()

        @dlt.transformer(
            data_from=outbox,
            name=stream.table,
            write_disposition="merge",
            primary_key=("id", "sequence"),
            columns=cols.hints,
        )
        def events(page: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
            ids = run.touched.get(int(page[0]["sequence"]), {}).get(stream.table, set()) - seen
            if not ids:
                return
            seen.update(ids)
            yield from snapshot.pages(
                f"select {cols.select_list} from {stream.table} "
                f"where id = any(%s::uuid[]) order by id, sequence",
                [sorted(ids)],
                page_size,
            )

        return events

    resources: list[Any] = [outbox]
    for stream in STREAMS:
        cols = stream_cols[stream.table]
        if watermark == 0:
            # First load: one sequential scan beats a keyed fetch per page.
            resources.append(
                dlt.resource(
                    scan(stream.table, cols, "id, sequence"),
                    name=stream.table,
                    write_disposition="merge",
                    primary_key=("id", "sequence"),
                    columns=cols.hints,
                )
            )
        else:
            resources.append(keyed(stream, cols))

    for table in STATE:
        cols = columns(conn, table)
        resources.append(
            dlt.resource(
                scan(table, cols, "1"),
                name=table,
                write_disposition="replace",
                columns=cols.hints,
            )
        )
    return resources


__all__ = ["Snapshot", "RunState", "cala_source", "resolve_watermark", "STATE", "STREAMS", "OUTBOX"]
