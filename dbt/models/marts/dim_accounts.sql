-- grain: one row per account VERSION (account_id, sequence)  — SCD type 2
--
-- Every event on an account is a version: `initialized` opens the first one,
-- each `updated` closes the previous and opens the next. Because the source is
-- an event stream we never diff columns to detect change; the change is the
-- event, and cala tells us which attributes moved in `changed_fields`.
--
-- valid_from / valid_to are half-open: [valid_from, valid_to).
with events as (

    select * from {{ ref('stg_cala_accounts') }}

),

versioned as (

    select
        account_id,
        sequence,
        version,
        event_type,
        changed_fields,
        code,
        name,
        external_id,
        description,
        normal_balance_type,
        status,
        is_account_set,
        eventually_consistent,
        metadata,
        recorded_at                                             as valid_from,
        lead(recorded_at) over (
            partition by account_id order by sequence
        )                                                       as valid_to,
        event_context
    from events

),

final as (

    select
        account_id || ':' || cast(sequence as {{ dbt.type_string() }})
                                                                as account_version_key,
        account_id,
        sequence                                                as version_number,
        version                                                 as cala_version,
        code,
        name,
        external_id,
        description,
        normal_balance_type,
        status,
        is_account_set,
        eventually_consistent,
        metadata,
        event_type,
        changed_fields,
        valid_from,
        valid_to,
        valid_to is null                                        as is_current,
        event_context
    from versioned

)

select * from final
