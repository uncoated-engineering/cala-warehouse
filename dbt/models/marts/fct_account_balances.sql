-- grain: one row per (journal_id, account_id, currency, layer)
--
-- Balances rebuilt from scratch by summing fct_entries. This is deliberately
-- independent of cala's own cala_current_balances projection: two
-- computations of the same number from the same entries, compared by
-- assert_balances_reconcile.
--
-- The grain includes `layer`. Settled, pending and encumbrance are separate
-- ledgers that happen to share an account; summing across them is a bug
-- unless you are explicitly producing an "available" figure.
--
-- Only accounts that entries post to appear here (cala forbids entries on
-- account-set backing accounts). Set balances are rolled up separately.
with entries as (

    select * from {{ ref('fct_entries') }}

),

accounts as (

    select
        account_id,
        normal_balance_type
    from {{ ref('stg_cala_accounts') }}
    qualify row_number() over (partition by account_id order by sequence desc) = 1

),

aggregated as (

    select
        journal_id,
        account_id,
        currency,
        layer,
        sum(debit_units)                                        as dr_balance,
        sum(credit_units)                                       as cr_balance,
        count(*)                                                as entry_count,
        min(recorded_at)                                        as first_entry_at,
        max(recorded_at)                                        as last_entry_at
    from entries
    group by 1, 2, 3, 4

),

latest_entry as (

    select
        journal_id,
        account_id,
        currency,
        layer,
        entry_id                                                as latest_entry_id
    from entries
    qualify row_number() over (
        partition by journal_id, account_id, currency, layer
        order by recorded_at desc, entry_sequence desc, entry_id desc
    ) = 1

),

final as (

    select
        a.journal_id,
        a.account_id,
        a.currency,
        a.layer,
        a.dr_balance,
        a.cr_balance,
        -- Signed by the account's normal balance side: positive means the
        -- account holds a balance on its natural side.
        case
            when acc.normal_balance_type = 'debit' then a.dr_balance - a.cr_balance
            else a.cr_balance - a.dr_balance
        end                                                     as balance,
        acc.normal_balance_type,
        a.entry_count,
        l.latest_entry_id,
        a.first_entry_at,
        a.last_entry_at
    from aggregated as a
    left join accounts as acc
        on acc.account_id = a.account_id
    left join latest_entry as l
        on  l.journal_id = a.journal_id
        and l.account_id = a.account_id
        and l.currency   = a.currency
        and l.layer      = a.layer

)

select * from final
