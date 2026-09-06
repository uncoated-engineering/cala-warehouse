-- Invariant (stricter): debits equal credits within every transaction, for
-- every currency AND every layer independently.
--
-- cala does not document this as a rule; the documented invariant is per
-- (transaction, currency). It should hold in practice because each entry
-- carries its own layer and a template's entries are written in balanced
-- pairs on the same layer. This test checks that empirically against the
-- fixtures. If it ever fails, that is a finding worth writing up, not a
-- reason to delete the test.
select
    transaction_id,
    currency,
    layer,
    sum(debit_units)                                            as total_debits,
    sum(credit_units)                                           as total_credits,
    sum(debit_units) - sum(credit_units)                        as difference
from {{ ref('fct_entries') }}
group by 1, 2, 3
having sum(debit_units) <> sum(credit_units)
