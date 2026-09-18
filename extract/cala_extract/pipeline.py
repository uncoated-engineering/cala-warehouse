"""Build the dlt pipeline for a destination and run one extraction.

`extract()` is the whole API: open a snapshot, run the source, close the
snapshot, and return a `RunSummary` the CLI prints and the tests assert on.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import dlt

from cala_extract.erasure import (
    FORGET_EVENT_TYPES,
    LogRow,
    Store,
    consume_forget_events,
    known_erasures,
    reapply,
    write_log,
)
from cala_extract.paths import DEFAULT_DATASET, DEFAULT_DUCKDB, DEFAULT_PIPELINES_DIR
from cala_extract.source import OUTBOX, RunState, Snapshot, cala_source
from cala_extract.tables import ALL_TABLES, STATE, STREAMS

DESTINATIONS = ("duckdb", "bigquery")


@dataclass
class RunSummary:
    destination: str
    dataset: str
    watermark_before: int
    watermark_after: int
    highest_seen: int
    stalled_at: int | None
    abandoned: list[int]
    # rows written per table this run (streams: new rows; state: the whole table)
    rows_loaded: dict[str, int]
    # table -> (source rows in the snapshot, destination rows after the run)
    verified: dict[str, tuple[int, int]] = field(default_factory=dict)
    # entities newly erased because this run landed a forget event for them
    forget_events: int = 0
    # table -> rows this run landed that a standing erasure had to redact again
    erasures_reapplied: dict[str, int] = field(default_factory=dict)
    # erasures on record after the run (original requests, not re-applies)
    erasures_known: int = 0

    @property
    def verification_failures(self) -> dict[str, tuple[int, int]]:
        return {t: c for t, c in self.verified.items() if c[0] != c[1]}

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["verification_failures"] = self.verification_failures
        return d


def pipeline_name(destination: str) -> str:
    return f"cala_extract_{destination}"


def build_pipeline(
    destination: str = "duckdb",
    *,
    duckdb_path: Path | str = DEFAULT_DUCKDB,
    dataset: str = DEFAULT_DATASET,
    pipelines_dir: Path | str = DEFAULT_PIPELINES_DIR,
) -> dlt.Pipeline:
    if destination == "duckdb":
        dest: Any = dlt.destinations.duckdb(credentials=str(duckdb_path))
    elif destination == "bigquery":
        # Same env vars as dbt/profiles.yml; credentials from the environment
        # (application default credentials), the way dbt's oauth method works.
        dest = dlt.destinations.bigquery(
            project_id=os.environ["DBT_BIGQUERY_PROJECT"],
            location=os.environ.get("DBT_BIGQUERY_LOCATION", "US"),
        )
    else:
        raise ValueError(f"unknown destination {destination!r}; one of {DESTINATIONS}")
    return dlt.pipeline(
        pipeline_name=pipeline_name(destination),
        destination=dest,
        dataset_name=dataset,
        pipelines_dir=str(pipelines_dir),
    )


class ForeignTables(Exception):
    """The dataset holds tables with our names that cala-extract did not land."""


def foreign_tables(pipeline: dlt.Pipeline) -> list[str]:
    """Tables in the dataset that carry our names but were not written by
    dlt (no `_dlt_load_id` column). Locally that means dbt seeds: `make build`
    and `make extract` both land `raw.cala_*`, and dlt would try to ALTER the
    seed tables into shape rather than replace them."""
    found: list[str] = []
    with pipeline.destination_client() as client:
        if not client.is_storage_initialized():
            return found
        for table in ALL_TABLES:
            exists, cols = client.get_storage_table(table)
            if exists and "_dlt_load_id" not in cols:
                found.append(table)
    return found


def drop_tables(pipeline: dlt.Pipeline, tables: list[str]) -> None:
    with pipeline.sql_client() as client:
        for table in tables:
            client.execute_sql(f"drop table if exists {client.make_qualified_table_name(table)}")


def stored_watermark(pipeline: dlt.Pipeline) -> int:
    """The outbox watermark the previous run committed, after syncing local
    state with the destination (so a fresh checkout resumes where the
    warehouse is, and a wiped warehouse starts over)."""
    pipeline.sync_destination()
    resources = pipeline.state.get("sources", {}).get("cala", {}).get("resources", {})
    return int(resources.get(OUTBOX, {}).get("watermark", 0))


def extract(
    source_url: str,
    *,
    destination: str = "duckdb",
    duckdb_path: Path | str = DEFAULT_DUCKDB,
    dataset: str = DEFAULT_DATASET,
    pipelines_dir: Path | str = DEFAULT_PIPELINES_DIR,
    full_refresh: bool = False,
    verify: bool = False,
    page_size: int = 5000,
    forget_event_types: tuple[str, ...] = FORGET_EVENT_TYPES,
) -> RunSummary:
    pipeline = build_pipeline(
        destination, duckdb_path=duckdb_path, dataset=dataset, pipelines_dir=pipelines_dir
    )
    if foreign := foreign_tables(pipeline):
        if not full_refresh:
            raise ForeignTables(
                f"{dataset}.{', '.join(foreign)} exist but were not landed by cala-extract "
                "(dbt seeds?). Run `make clean` to start from an empty warehouse, or pass "
                "--full-refresh to drop and reload them."
            )
        drop_tables(pipeline, foreign)
    watermark = 0 if full_refresh else stored_watermark(pipeline)

    run = RunState()
    snapshot = Snapshot.open(source_url)
    try:
        source = cala_source(
            snapshot, run, source_url=source_url, watermark=watermark, page_size=page_size, verify=verify
        )
        info = pipeline.run(source, refresh="drop_resources" if full_refresh else None)
    finally:
        snapshot.close()

    normalize = pipeline.last_trace.last_normalize_info
    counts = normalize.row_counts if normalize else {}
    rows_loaded = {t: int(counts.get(t, 0)) for t in ALL_TABLES}

    # Erasure propagation, on exactly the rows this run landed: honour any
    # forget event that arrived, then re-apply every standing erasure (the
    # state tables were just replaced from a source that still holds the
    # data, and a re-announced entity's events were re-merged). See erasure.py.
    load_ids = list(info.loads_ids)
    run_id = load_ids[-1] if load_ids else f"cala-extract:{os.getpid()}"
    log_rows: list[LogRow] = []
    with Store(pipeline) as store:
        forgotten = consume_forget_events(store, load_ids, run_id, forget_event_types)
        log_rows += forgotten
        reapplied = reapply(store, load_ids, run_id)
        log_rows += reapplied
    write_log(pipeline, log_rows)
    erasures_reapplied: dict[str, int] = {}
    for row in reapplied:
        erasures_reapplied[row.table_name] = erasures_reapplied.get(row.table_name, 0) + row.rows_redacted
    with Store(pipeline) as store:
        erasures_known = len(known_erasures(store))

    verified: dict[str, tuple[int, int]] = {}
    if verify:
        with pipeline.sql_client() as client:
            for table, source_rows in run.source_counts.items():
                qualified = client.make_qualified_table_name(table)
                dest_rows = client.execute_sql(f"select count(*) from {qualified}")[0][0]
                verified[table] = (source_rows, int(dest_rows))

    return RunSummary(
        destination=destination,
        dataset=dataset,
        watermark_before=run.watermark_before,
        watermark_after=run.watermark_after,
        highest_seen=run.highest_seen,
        stalled_at=run.stalled_at,
        abandoned=run.abandoned,
        rows_loaded=rows_loaded,
        verified=verified,
        forget_events=len({r.erasure_id for r in forgotten}),
        erasures_reapplied=erasures_reapplied,
        erasures_known=erasures_known,
    )


def format_summary(s: RunSummary) -> str:
    lines = [f"destination: {s.destination} / {s.dataset}"]
    lines.append(
        f"outbox watermark: {s.watermark_before} -> {s.watermark_after}"
        + (f" (highest seen {s.highest_seen})" if s.highest_seen != s.watermark_after else "")
    )
    if s.stalled_at is not None:
        lines.append(
            f"stalled at sequence {s.stalled_at}: in flight or committed after the snapshot; "
            "the next run picks it up"
        )
    if s.abandoned:
        lines.append(f"abandoned sequences skipped: {s.abandoned}")
    width = max(len(t) for t in ALL_TABLES)
    lines.append("")
    lines.append(f"{'table':<{width}}  disposition  rows loaded" + ("  source  dest" if s.verified else ""))
    stream_tables = {OUTBOX, *(st.table for st in STREAMS)}
    for table in ALL_TABLES:
        disp = "merge" if table in stream_tables else "replace"
        line = f"{table:<{width}}  {disp:<11}  {s.rows_loaded.get(table, 0):>11}"
        if table in s.verified:
            src, dst = s.verified[table]
            line += f"  {src:>6}  {dst:>4}" + ("" if src == dst else "  MISMATCH")
        lines.append(line)
    if s.verified:
        lines.append("")
        lines.append(
            "verify: OK, every table matches the snapshot"
            if not s.verification_failures
            else f"verify: FAILED for {sorted(s.verification_failures)}"
        )
    if s.erasures_known or s.forget_events:
        lines.append("")
        lines.append(
            f"erasures: {s.erasures_known} on record, {s.forget_events} new from forget events this run"
            + (
                ", re-applied on landed rows: "
                + ", ".join(f"{t} {n}" for t, n in sorted(s.erasures_reapplied.items()))
                if s.erasures_reapplied
                else ", nothing to re-apply"
            )
        )
    return "\n".join(lines)


__all__ = ["ForeignTables", "RunSummary", "build_pipeline", "extract", "format_summary", "stored_watermark", "STATE"]
