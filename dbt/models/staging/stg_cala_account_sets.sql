-- grain: one row per account-set event (id, sequence)
--
-- An account set is also an account (cala creates a backing row in
-- cala_accounts with is_account_set = true), so account_set_id joins to
-- stg_cala_accounts.account_id.
with source as (

    select * from {{ source('cala', 'cala_account_set_events') }}

),

renamed as (

    select
        id                                                      as account_set_id,
        sequence,
        event_type,
        {{ json_string_array('event', 'fields') }}              as changed_fields,
        {{ json_int('event', 'values.version') }}               as version,
        {{ json_string('event', 'values.journal_id') }}         as journal_id,
        {{ json_string('event', 'values.name') }}               as name,
        {{ json_string('event', 'values.external_id') }}        as external_id,
        {{ json_string('event', 'values.description') }}        as description,
        {{ json_string('event', 'values.normal_balance_type') }} as normal_balance_type,
        {{ json_object('event', 'values.metadata') }}           as metadata,
        recorded_at,
        context                                                 as event_context
    from source

)

select * from renamed
