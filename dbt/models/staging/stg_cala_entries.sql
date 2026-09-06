{{
    config(
        materialized = 'incremental',
        unique_key   = ['entry_id', 'sequence'],
        on_schema_change = 'append_new_columns'
    )
}}

-- grain: one row per entry event (id, sequence). In practice one per entry:
-- cala only ever emits `initialized` for entries.
--
-- This is the append-only / incremental pattern. cala_entry_events has
-- UNIQUE(id, sequence) and is never updated in place, so replaying on that key
-- is idempotent and a watermark is safe. The watermark is PER ENTITY ID:
-- every new entity starts at sequence 1, so a global max(sequence) would
-- skip every entity created after the first load.
with source as (

    select * from {{ source('cala', 'cala_entry_events') }}

    {% if is_incremental() %}
    where sequence > coalesce(
        (select max(t.sequence) from {{ this }} as t
         where t.entry_id = {{ source('cala', 'cala_entry_events') }}.id),
        0
    )
    {% endif %}

),

renamed as (

    select
        id                                                      as entry_id,
        sequence,
        event_type,
        {{ json_int('event', 'values.version') }}               as version,
        {{ json_string('event', 'values.transaction_id') }}     as transaction_id,
        {{ json_string('event', 'values.journal_id') }}         as journal_id,
        {{ json_string('event', 'values.account_id') }}         as account_id,
        {{ json_string('event', 'values.entry_type') }}         as entry_type,
        {{ json_int('event', 'values.sequence') }}              as entry_sequence,
        lower({{ json_string('event', 'values.layer') }})       as layer,
        lower({{ json_string('event', 'values.direction') }})   as direction,
        {{ json_decimal('event', 'values.units') }}             as units,
        {{ json_string('event', 'values.currency') }}           as currency,
        {{ json_string('event', 'values.description') }}        as description,
        {{ json_object('event', 'values.metadata') }}           as metadata,
        recorded_at,
        context                                                 as event_context
    from source

)

select * from renamed
