-- Invariant: within every transaction, for every currency, total debit units
-- equal total credit units. This is double-entry bookkeeping itself. A row
-- returned here means cala posted an unbalanced transaction, which would be a
-- bug in the ledger, not in the warehouse.
--
-- Grain of the check: (transaction_id, currency). The stricter per-layer
-- version lives in assert_debits_equal_credits_per_layer.
select
    transaction_id,
    currency,
    sum(debit_units)                                            as total_debits,
    sum(credit_units)                                           as total_credits,
    sum(debit_units) - sum(credit_units)                        as difference
from {{ ref('fct_entries') }}
group by 1, 2
having sum(debit_units) <> sum(credit_units)
