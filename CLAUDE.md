# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A portable dbt package that models event-sourced ledger data produced by
[`es-entity`](https://github.com/GaloyMoney/es-entity) /
[`cala-ledger`](https://github.com/GaloyMoney/cala), plus a DuckDB harness so the
whole package builds and tests locally from committed seeds with no cloud
credentials, plus an MCP server (`mcp/`) that exposes ledger semantics over
the built marts to an agent, plus an extractor (`extract/`) that lands the
same tables from a live cala Postgres with the outbox as the change log.

The deliverable is the **tests**, not the models: a reconciliation suite that
independently recomputes balances from raw entries and compares them to the
balances cala itself persisted.

## Build and Development Commands

### Setup
- `make install` — `uv sync`; installs dbt-core + dbt-duckdb into `.venv/`
- Python is pinned in `.python-version`; `uv` manages everything

### dbt (run from repo root, all target DuckDB by default)
- `make seed` — load `fixtures/seed/*.csv` into DuckDB (`dbt seed`)
- `make run` — build models only
- `make test` — run tests only
- `make build` — `dbt seed`, then `dbt build --exclude resource_type:seed`. This is what CI runs.
  The two steps are deliberate: staging reads seeds through `source()`, which
  dbt does not order after seeds, so one `dbt build` races on a fresh database.
- `make build FULL_REFRESH=1` — rebuild incremental models. Required after `make fixtures`
  (the seeds are replaced, and `stg_cala_entries` would otherwise keep the old batch).
- `make docs` — generate the dbt docs site
- `TARGET=bigquery make build` — same package against BigQuery (needs credentials, see `dbt/profiles.yml`)
- Single model: `cd dbt && uv run dbt build --select fct_entries+`
- Single test: `cd dbt && uv run dbt test --select assert_balances_reconcile`
- Bounded load: `--vars '{"entries_as_of": "2026-01-01T00:00:00"}'` stops `stg_cala_entries` at a watermark
- Benchmark: `uv run python scripts/benchmark_incremental.py` (needs a seeded database)

### MCP server (mcp/)
- `make mcp` — start the read-only MCP server on stdio (`uv run cala-mcp`). Needs `make build` first.
- `make mcp-test` — `uv run pytest`; the suite reads `dbt/target/manifest.json` and
  `dbt/cala_warehouse.duckdb` and skips if they are missing.
- Single test: `uv run pytest mcp/tests/test_reconcile.py -k green`
- Call a tool without MCP: `uv run python -c "from cala_mcp.tools import reconcile; print(reconcile()['mismatches'])"`
- `CALA_WAREHOUSE_MANIFEST`, `CALA_WAREHOUSE_DUCKDB` — override the artefact paths

### Extractor (extract/)
- `make fixture-pg` — Docker Postgres on 5434 loaded with `fixtures/schema.sql` + the seeds; `make fixture-pg-stop` removes it
- `make extract` — `uv run cala-extract --verify` from `$CALA_PG_URL` (defaults to the fixture Postgres)
  into `dbt/cala_warehouse.duckdb`, schema `raw`. `make clean` first if `make build` (seeds) ran on that file.
- `make build-extracted` — `dbt build --exclude resource_type:seed` on what was landed
- `make extract-test` — `uv run pytest extract/tests`; skips without `CALA_PG_URL`. DESTRUCTIVE on that
  database: every test drops and recreates its `public` schema.
- Single test: `uv run pytest extract/tests -k in_flight`
- `uv run cala-extract --json`, `--full-refresh`, `--destination bigquery`, `--page-size N`
- `uv run python -m cala_extract.fixture_db $URL` — load schema + seeds into any Postgres
- `uv run cala-extract --forget-event-types forgot,erased` — event types that file an erasure (default `forgot`)

### Erasure (extract/cala_extract/erasure.py)
- `make erase ARGS="account <uuid> --reason DSAR-42 --requested-by ops --with-entries"` — `cala-erase`, then
  `dbt build --exclude resource_type:seed` so the marts are re-materialised and the control runs
- `make erasures` — print `raw.cala_erasure_log`; `uv run cala-erase reapply` re-redacts every logged erasure on every row
- `uv run cala-erase account-set|transaction|entry|journal <uuid> --reason ...`
- Tests: `uv run pytest extract/tests/test_erase.py`; the first half runs on a DuckDB file filled from the seeds
  (no Postgres), the Postgres half (`-k "reapplied or forget"`) needs `CALA_PG_URL`

### Fixtures
- `make fixtures` — regenerate `fixtures/seed/` by running cala's own integration
  test suite in Docker and dumping the resulting tables. Needs Docker and a
  checkout of cala at `../cala` (override with `CALA_DIR=...`).
- Never hand-edit seed CSVs. They must come from cala's code so the schemas
  and event sequences are real. `fixtures/MANIFEST.md` records which cala
  produced them and what its test suite reported. `fixtures/schema.sh`
  (called by `generate.sh`) dumps `fixtures/schema.sql` from cala's migrations;
  never hand-edit that either.
- Seeds are disabled off the `duckdb` target so fixtures never land on a real warehouse.

## Architecture Overview

```
cala Postgres ──cala-extract──┐   (production path; extract/cala_extract/)
fixtures/seed/*.csv (dbt seeds)┤   (local / CI path)
        │
   raw.cala_*              the same 17 tables either way
        │
dbt/models/staging/        one model per entity event stream; unpacks event JSON
        │                  into typed columns, keeps `context` for audit lineage
dbt/models/marts/          dims, facts, and the trial balance report
        │
dbt/tests/                 singular tests = the accounting controls
        │
mcp/cala_mcp/              MCP server over dbt/target/manifest.json + the DuckDB file
  manifest.py              Manifest / Relation: models, columns, grain, lineage; require() gate
  db.py                    read-only DuckDB connection; Decimal -> exact string
  tools.py                 describe_model, list_models, trial_balance,
                           explain_account_balance, reconcile as plain functions
  server.py                MCPServer registration, ToolError mapping, stdio entrypoint
mcp/tests/                 pytest over the built warehouse (no fixtures of its own)

extract/cala_extract/      dlt source over cala's Postgres
  tables.py                what is a stream (outbox-keyed, merge) vs state (replace);
                           column discovery + casts from information_schema
  source.py                Snapshot (REPEATABLE READ + xid marker), outbox resource with
                           the contiguity watermark, keyed transformers, resolve_watermark
  pipeline.py              build_pipeline (duckdb | bigquery), extract(), RunSummary, seed guard
  cli.py                   `cala-extract`
  fixture_db.py            load schema.sql + seeds into a Postgres (tests, CI, make fixture-pg)
extract/tests/             pytest against that Postgres; needs CALA_PG_URL
```

### The controls (dbt/tests/)
| test | invariant |
|---|---|
| `assert_debits_equal_credits` | per (transaction, currency), Σ debit units = Σ credit units |
| `assert_debits_equal_credits_per_layer` | same per (transaction, currency, layer); undocumented in cala, holds empirically |
| `assert_no_sequence_gaps` | per entity id, sequence = 1..n on every staged stream (completeness) |
| `assert_balances_reconcile` | `fct_account_balances` = `stg_cala_balances` on the full grain, exact decimals |
| `assert_account_set_balances_reconcile` | cala's set balances = rollup through `dim_account_set_members`, scoped to the set's journal |
| `assert_erased_entities_hold_no_personal_data` | for every entity in `stg_cala_erasures`, no personal field holds a value in staging, the marts that carry it, the raw state tables or the outbox payloads |

### Things that are easy to get wrong here
- Account-set membership is NOT in `cala_account_set_events`; it only exists on
  the outbox. `stg_cala_account_set_members` reads `cala_persistent_outbox_events`.
- A set belongs to one journal, an account does not. cala rolls a posting only
  into ancestor sets in the posting's journal. Any set-level aggregate must
  join the set's `journal_id`.
- cala writes all three layers on first touch of an (account, currency), so a
  0/0 layer on cala's side with no entries on ours is a match, not a gap.
- `layer` and `direction` arrive PascalCase in event JSON (`Settled`) but
  lowercase in Postgres enums; staging lowercases them.
- `context` is null in the fixtures (cala's tests set none). Keep the column anyway.

### Extractor rules (extract/)
- The cursor is the outbox `sequence`, never `recorded_at` (caller-supplied
  and backdatable; see README "Extraction"). The watermark advances only
  across contiguous sequences; a hole is skipped only after the xmin-horizon
  proof in `resolve_watermark`, which mirrors obix's `abandonment_proof_passed`.
  The marker is a real xid from an auto-commit statement taken after the
  snapshot; the snapshot's own xmax is not a valid marker.
- Everything is read from one REPEATABLE READ snapshot so streams and state
  agree. Do not add a resource that opens its own connection for data.
- Streams (`*_events` + outbox) are `merge` on their unique key; state tables
  are `replace`. A new `*_events` table becomes a `Stream` in `tables.py` only
  with the outbox payload types that announce it; if the outbox does not
  announce every write to it (templates), it is state.
- Column lists come from information_schema with one cast per type family so
  the landed shape equals the seeds: UUID / JSON / enum as text, timestamptz
  as naive UTC `timestamp`. Every column gets a dlt type hint so all-NULL
  columns (`context`) still exist downstream.
- The extractor never writes to the source: the snapshot is READ ONLY and
  the proof connection only reads.
- Tests assert on `RunSummary` (watermarks, `rows_loaded`, `stalled_at`,
  `abandoned`, `verification_failures`) and on row-for-row equality with the
  seeds via `warehouse_checks.assert_raw_equals_seeds`.

### Erasure rules (extract/cala_extract/erasure.py)
- An erasure is targeted deletion of the personal fields in `KINDS` (name,
  description, external_id, metadata, as far as the entity has them) in every
  landed copy: the `*_events` JSON, the state row, the outbox payload. It never
  touches an amount, id, sequence, timestamp or `fields` array, so the
  reconciliation controls are unaffected; `assert_erased_entities_hold_no_personal_data`
  mirrors `KINDS` field for field. Change one, change the other.
- The log (`cala_erasure_log`) is append-only and is its own dlt source
  (`cala_warehouse`), so `--full-refresh` (which drops the `cala` source's
  resources) never drops it. It records tables, fields and row counts, never
  values and never hashes of values.
- Every `extract()` re-applies the log to the rows the run landed
  (`_dlt_load_id in loads_ids`) and logs a `reapply` row only when something
  was actually redacted again. Do not add a load path that bypasses
  `reapply`, and keep the personal fields out of any new table's replace path
  unless `KINDS` covers it.
- Forget detection reads `event_type in FORGET_EVENT_TYPES` on the rows a run
  landed and files one erasure per entity, once (`source = 'forget_event'`).
  cala emits no such event; the tests insert one the way an es-entity service
  would (`Forgot {}` staged before `forget()`, announced on the outbox).
- `Store` runs on dlt's sql client with `%s` placeholders (dlt rewrites them
  per destination); no destination-specific SQL, no JSON functions in SQL:
  the JSON is rewritten in Python row by row, so the same code runs on
  DuckDB and BigQuery.
- `stg_cala_entries` re-selects erased entry ids on every incremental run;
  the other staging models are views and need nothing. A mart that carries a
  personal field must be listed in the control's `holders`.

### MCP server rules (mcp/)
- Every identifier in a query comes from the manifest: `manifest.model(name)`
  for the relation, `rel.col(name)` / `rel.require(...)` for columns,
  `rel.grain` for the grain, `rel.accepted_values(col)` for enums. No
  hardcoded column lists; if a tool needs a column, describe it in the model's
  yml so the manifest carries it (`test_every_mart_documents_every_physical_column`
  enforces this for marts).
- No SQL tool, no write path. `open_warehouse()` is `read_only=True`; keep it so.
- Amounts are `Decimal` in DuckDB and exact strings in responses (`db.decimal_str`).
  Never `float()` a balance.
- Tool functions live in `tools.py` and take `manifest_path` / `duckdb_path`
  keyword arguments so tests can point them anywhere; `server.py` only wires
  them up. Raise `ManifestError` / `ValueError` for caller-fixable problems;
  `server.anticipated` turns those into `ToolError` so the agent sees the text.
- `mcp/` has no `__init__.py` on purpose: the importable package is
  `cala_mcp`, so the PyPI `mcp` package is never shadowed.
- The MCP SDK is 2.x: `MCPServer`, snake_case result fields (`is_error`,
  `structured_content`, `input_schema`).

### Source data contract (from es-entity)
Every entity has two tables:
- `cala_<entity>s` — mutable current state, updated in place
- `cala_<entity>_events` — append-only stream with `UNIQUE(id, sequence)`

Model **incrementally** on `*_events` (safe: append-only, replayable on `(id, sequence)`).
Model **full-refresh** on the current-state tables (they mutate; sequence watermarks are wrong there).

### The balance grain
Balances live at `(journal_id, account_id, currency, layer)`. Layer is one of
`settled | pending | encumbrance`. Never aggregate across layers unless a model
explicitly says it is producing an "available" balance.

`units` is a decimal. It must stay `DECIMAL(38, 18)` (DuckDB) / `BIGNUMERIC`
(BigQuery) end to end. Any cast to DOUBLE/FLOAT is a bug.

## Environment Variables

- `DBT_TARGET` — not used; pass `--target` or `make TARGET=...`
- `DBT_PROFILES_DIR` — defaults to `dbt/` (a `profiles.yml` is committed there)
- `CALA_DIR` — path to a cala checkout for `make fixtures` (default `../cala`)
- `CALA_PG_URL` — the Postgres `cala-extract` and `extract/tests` read (Makefile default: the fixture Postgres on 5434)
- `CALA_EXTRACT_PIPELINES_DIR` — dlt's local state (default `.dlt/pipelines`, gitignored)
- `FIXTURE_PG_PORT` — port for `make fixture-pg` (default 5434)
- `DBT_BIGQUERY_PROJECT`, `DBT_BIGQUERY_DATASET` — only for the `bigquery` target

## Code Style Guide

### SQL
- Lowercase keywords. One column per line. Trailing commas never; leading commas never — one column per line makes both unnecessary.
- Every model starts with a comment stating its **grain** in one line.
- Structure models as `with ... as (...)` CTEs: `source` / `renamed` / `final`.
  A CTE does one thing.
- Reference upstream with `{{ ref() }}` / `{{ source() }}` only. No hard-coded
  schema names.
- Cross-database functions go through the macros in `dbt/macros/` so the same
  model compiles on DuckDB and BigQuery. Do not inline dialect-specific JSON
  extraction in a model.
- Prefer `qualify` / window functions over self-joins for "latest event per id".

### Naming
- `stg_cala_<entity>` — staging, one per source event stream
- `dim_<noun>` — dimensions (SCD-2 where history matters)
- `fct_<noun>` — facts at a stated grain
- `rpt_<noun>` — reports shaped for a human reader
- `assert_<invariant>` — singular tests; a test's name states the invariant it proves

### Tests
- Generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) go in `schema.yml` next to the model.
- Singular tests in `dbt/tests/` encode accounting invariants. Each one has a
  header comment explaining the invariant and what a failure would mean.
- A test that fails against the committed seeds is a finding, not something to
  loosen. Write it up in the README.

### Commits
- Conventional commits: `feat(scope):`, `fix(scope):`, `test:`, `docs:`, `chore:`.
- Scope is the layer: `fixtures`, `staging`, `marts`, `tests`, `ci`, `mcp`, `extract`, `erasure`.
