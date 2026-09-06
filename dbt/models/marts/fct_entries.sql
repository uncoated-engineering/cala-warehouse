-- grain: one row per ledger entry (one double-entry line)
--
-- The grain everything else sits on. debit_units / credit_units are split
-- out so balances and trial balances are plain sums with no CASE logic
-- downstream; exactly one of them is non-zero per row.
with entries as (

    select * from {{ ref('stg_cala_entries') }}
    -- cala only emits `initialized` for entries, but take the latest event
    -- per id anyway so a future `updated` event cannot double-count.
    qualify row_number() over (partition by entry_id order by sequence desc) = 1

),

transactions as (

    select
        transaction_id,
        effective_date,
        tx_created_at
    from {{ ref('stg_cala_transactions') }}
    qualify row_number() over (partition by transaction_id order by sequence desc) = 1

),

final as (

    select
        e.entry_id,
        e.transaction_id,
        e.journal_id,
        e.account_id,
        e.currency,
        e.layer,
        e.direction,
        e.units,
        case when e.direction = 'debit'  then e.units else 0 end as debit_units,
        case when e.direction = 'credit' then e.units else 0 end as credit_units,
        e.entry_type,
        e.entry_sequence,
        e.description,
        e.metadata,
        t.effective_date,
        t.tx_created_at                                         as transaction_created_at,
        e.recorded_at,
        e.event_context
    from entries as e
    left join transactions as t
        on t.transaction_id = e.transaction_id

)

select * from final
