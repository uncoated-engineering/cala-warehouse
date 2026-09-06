-- grain: one row per transaction
--
-- Current state of each transaction (latest event) plus entry-level
-- aggregates. `is_balanced` is the double-entry invariant evaluated per
-- currency; assert_debits_equal_credits is the test that enforces it.
with transactions as (

    select * from {{ ref('stg_cala_transactions') }}
    qualify row_number() over (partition by transaction_id order by sequence desc) = 1

),

per_currency as (

    select
        transaction_id,
        currency,
        sum(debit_units)                                        as debit_units,
        sum(credit_units)                                       as credit_units
    from {{ ref('fct_entries') }}
    group by 1, 2

),

entry_aggregates as (

    select
        transaction_id,
        count(*)                                                as entry_count,
        count(distinct currency)                                as currency_count,
        count(distinct layer)                                   as layer_count,
        count(distinct account_id)                              as account_count
    from {{ ref('fct_entries') }}
    group by 1

),

balanced as (

    select
        transaction_id,
        {{ bool_and_agg('debit_units = credit_units') }}        as is_balanced
    from per_currency
    group by 1

),

final as (

    select
        t.transaction_id,
        t.journal_id,
        t.tx_template_id,
        t.external_id,
        t.correlation_id,
        t.effective_date,
        t.description,
        t.metadata,
        t.version,
        t.sequence                                              as event_count,
        coalesce(a.entry_count, 0)                              as entry_count,
        coalesce(a.currency_count, 0)                           as currency_count,
        coalesce(a.layer_count, 0)                              as layer_count,
        coalesce(a.account_count, 0)                            as account_count,
        b.is_balanced,
        t.tx_created_at                                         as created_at,
        t.tx_modified_at                                        as modified_at,
        t.event_context
    from transactions as t
    left join entry_aggregates as a
        on a.transaction_id = t.transaction_id
    left join balanced as b
        on b.transaction_id = t.transaction_id

)

select * from final
