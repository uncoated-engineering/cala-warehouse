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
--
-- var entries_as_of (optional timestamp): only load events recorded at or
-- before it. Lets a backfill stop at a watermark, and lets the benchmark in
-- the README load the seeds in two batches.
--
-- Erasure: an erased entry's event JSON is redacted in place in raw, below
-- the watermark, so the incremental run would never see it again. Every
-- entry id in the erasure log is re-selected on each run and replaces the
-- stale row on the unique key. Erasures are rare, so this costs nothing.
-- depends_on: {{ ref('stg_cala_erasures') }}
with

{% if is_incremental() %}
watermark as (

    select
        entry_id,
        max(sequence)                                           as max_sequence
    from {{ this }}
    group by 1

),

erased as (

    select entity_id
    from {{ ref('stg_cala_erasures') }}
    where entity_kind = 'entry'

),
{% endif %}

source as (

    select s.*
    from {{ source('cala', 'cala_entry_events') }} as s
    {% if is_incremental() %}
    left join watermark as w
        on w.entry_id = s.id
    {% endif %}
    where 1 = 1
    {% if is_incremental() %}
      and (
          s.sequence > coalesce(w.max_sequence, 0)
          or s.id in (select entity_id from erased)
      )
    {% endif %}
    {% if var('entries_as_of', none) %}
      and s.recorded_at <= cast('{{ var("entries_as_of") }}' as timestamp)
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
