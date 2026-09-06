# cala-warehouse

A portable dbt package for modelling event-sourced ledger data from the Galoy
stack ([`es-entity`](https://github.com/GaloyMoney/es-entity) /
[`cala-ledger`](https://github.com/GaloyMoney/cala)), with a DuckDB harness so
the whole thing builds and tests locally from committed seeds.

**Status:** work in progress. See `CLAUDE.md` for layout and conventions.

## Quick start

```sh
make install   # uv sync
make build     # dbt seed + run + test against DuckDB, no credentials needed
```
