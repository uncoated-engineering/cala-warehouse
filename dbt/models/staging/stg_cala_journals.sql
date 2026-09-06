-- grain: one row per journal event (id, sequence)
with source as (

    select * from {{ source('cala', 'cala_journal_events') }}

),

renamed as (

    select
        id                                                      as journal_id,
        sequence,
        event_type,
        {{ json_string_array('event', 'fields') }}              as changed_fields,
        {{ json_int('event', 'values.version') }}               as version,
        {{ json_string('event', 'values.name') }}               as name,
        {{ json_string('event', 'values.code') }}               as code,
        {{ json_string('event', 'values.description') }}        as description,
        {{ json_string('event', 'values.status') }}             as status,
        {{ json_bool('event', 'values.config.enable_effective_balances') }}
                                                                as enable_effective_balances,
        recorded_at,
        context                                                 as event_context
    from source

)

select * from renamed
