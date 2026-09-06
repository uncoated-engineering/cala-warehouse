-- grain: one row per (journal_id, currency, layer)
--
-- The report a finance reader expects: for every journal, currency and layer,
-- total debits, total credits, and the difference. In a correct double-entry
-- ledger the difference is exactly zero on every row. It is computed from
-- fct_account_balances, i.e. from the entries, not from cala's projections.
with balances as (

    select * from {{ ref('fct_account_balances') }}

),

journals as (

    select
        journal_id,
        name                                                    as journal_name,
        code                                                    as journal_code
    from {{ ref('stg_cala_journals') }}
    qualify row_number() over (partition by journal_id order by sequence desc) = 1

),

totals as (

    select
        journal_id,
        currency,
        layer,
        count(distinct account_id)                              as account_count,
        sum(entry_count)                                        as entry_count,
        sum(dr_balance)                                         as total_debits,
        sum(cr_balance)                                         as total_credits,
        sum(dr_balance) - sum(cr_balance)                       as difference,
        max(last_entry_at)                                      as as_of
    from balances
    group by 1, 2, 3

),

final as (

    select
        t.journal_id,
        j.journal_name,
        j.journal_code,
        t.currency,
        t.layer,
        t.account_count,
        t.entry_count,
        t.total_debits,
        t.total_credits,
        t.difference,
        t.difference = 0                                        as is_balanced,
        t.as_of
    from totals as t
    left join journals as j
        on j.journal_id = t.journal_id

)

select * from final
