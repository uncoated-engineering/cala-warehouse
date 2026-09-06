-- grain: one row per account event (id, sequence)
--
-- `changed_fields` is the `fields: Vec<String>` cala attaches to every
-- `updated` event: the exact list of attributes that changed. dim_accounts
-- uses it to build SCD-2 history without diffing columns.
with source as (

    select * from {{ source('cala', 'cala_account_events') }}

),

renamed as (

    select
        id                                                      as account_id,
        sequence,
        event_type,
        {{ json_string_array('event', 'fields') }}              as changed_fields,
        {{ json_int('event', 'values.version') }}               as version,
        {{ json_string('event', 'values.code') }}               as code,
        {{ json_string('event', 'values.name') }}               as name,
        {{ json_string('event', 'values.external_id') }}        as external_id,
        {{ json_string('event', 'values.description') }}        as description,
        {{ json_string('event', 'values.normal_balance_type') }} as normal_balance_type,
        {{ json_string('event', 'values.status') }}             as status,
        {{ json_bool('event', 'values.config.is_account_set') }} as is_account_set,
        {{ json_bool('event', 'values.config.eventually_consistent') }}
                                                                as eventually_consistent,
        {{ json_object('event', 'values.metadata') }}           as metadata,
        recorded_at,
        context                                                 as event_context
    from source

)

select * from renamed
