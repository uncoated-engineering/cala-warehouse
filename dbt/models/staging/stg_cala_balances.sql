-- grain: one row per (journal_id, account_id, currency, layer)
--
-- cala's OWN balance projection, unpivoted from the nested `latest_values`
-- JSON into the balance grain. Nothing downstream computes from this; it
-- exists so assert_balances_reconcile can compare cala's numbers with ours.
-- Debits and credits are kept as separate running totals exactly as cala
-- stores them (dr_balance / cr_balance), so the comparison is stricter than
-- a net figure.
with source as (

    select * from {{ source('cala', 'cala_current_balances') }}

),

{% set layers = ['settled', 'pending', 'encumbrance'] %}

unpivoted as (

    {% for layer in layers %}
    select
        journal_id,
        account_id,
        currency,
        '{{ layer }}'                                           as layer,
        latest_version                                          as balance_version,
        {{ json_decimal('latest_values', layer ~ '.dr_balance') }} as dr_balance,
        {{ json_decimal('latest_values', layer ~ '.cr_balance') }} as cr_balance,
        {{ json_string('latest_values', layer ~ '.entry_id') }}    as latest_entry_id,
        {{ json_timestamp('latest_values', layer ~ '.modified_at') }} as layer_modified_at,
        {{ json_timestamp('latest_values', 'modified_at') }}    as balance_modified_at
    from source
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select * from unpivoted
