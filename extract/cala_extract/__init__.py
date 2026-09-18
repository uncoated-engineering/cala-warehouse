"""cala-extract: land cala-ledger's Postgres tables in the warehouse.

Outbox-driven incremental loads for the append-only event streams, replace
for the mutable state tables, one consistent snapshot per run. See
source.py for the argument and tables.py for what is loaded how.
"""
