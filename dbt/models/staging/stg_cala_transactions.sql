-- grain: one row per transaction event (id, sequence)
with source as (

    select * from {{ source('cala', 'cala_transaction_events') }}

),

renamed as (

    select
        id                                                      as transaction_id,
        sequence,
        event_type,
        {{ json_string_array('event', 'fields') }}              as changed_fields,
        {{ json_int('event', 'values.version') }}               as version,
        {{ json_string('event', 'values.journal_id') }}         as journal_id,
        {{ json_string('event', 'values.tx_template_id') }}     as tx_template_id,
        {{ json_string('event', 'values.external_id') }}        as external_id,
        {{ json_string('event', 'values.correlation_id') }}     as correlation_id,
        cast({{ json_string('event', 'values.effective') }} as date)
                                                                as effective_date,
        {{ json_string('event', 'values.description') }}        as description,
        {{ json_object('event', 'values.metadata') }}           as metadata,
        {{ json_string_array('event', 'values.entry_ids') }}    as entry_ids,
        {{ json_timestamp('event', 'values.created_at') }}      as tx_created_at,
        {{ json_timestamp('event', 'values.modified_at') }}     as tx_modified_at,
        recorded_at,
        context                                                 as event_context
    from source

)

select * from renamed
