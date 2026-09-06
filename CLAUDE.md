# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A portable dbt package that models event-sourced ledger data produced by
[`es-entity`](https://github.com/GaloyMoney/es-entity) /
[`cala-ledger`](https://github.com/GaloyMoney/cala), plus a DuckDB harness so the
whole package builds and tests locally from committed seeds with no cloud
credentials.

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
- `make build` — seed + run + test in DAG order (`dbt build`). This is what CI runs.
- `make docs` — generate the dbt docs site
- `TARGET=bigquery make build` — same package against BigQuery (needs credentials, see `dbt/profiles.yml`)
- Single model: `cd dbt && uv run dbt build --select fct_entries+`
- Single test: `cd dbt && uv run dbt test --select assert_balances_reconcile`

### Fixtures
- `make fixtures` — regenerate `fixtures/seed/` by running cala's own integration
  test suite in Docker and dumping the resulting tables. Needs Docker and a
  checkout of cala at `../cala` (override with `CALA_DIR=...`).
- Never hand-edit seed CSVs. They must come from cala's code so the schemas
  and event sequences are real.

## Architecture Overview

```
fixtures/seed/*.csv        raw cala tables as dumped from Postgres (dbt seeds)
        │
dbt/models/staging/        one model per entity event stream; unpacks event JSON
        │                  into typed columns, keeps `context` for audit lineage
dbt/models/marts/          dims, facts, and the trial balance report
        │
dbt/tests/                 singular tests = the accounting controls
```

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
- Scope is the layer: `fixtures`, `staging`, `marts`, `tests`, `ci`.
