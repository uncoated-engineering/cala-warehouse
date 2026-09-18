{{ config(tags=['quality']) }}

-- grain: one row per (invocation_id, source_table)
--
-- Freshness of the two streams that drive the marts, as seen by each dbt
-- invocation: the newest recorded_at (and, for the outbox, the highest
-- sequence) and the lag from that to the invocation's start. recorded_at is
-- caller-supplied on es-entity tables, so backdated events inflate the lag;
-- the outbox's own recorded_at defaults to now() and is the better signal.
with source as (

    select * from {{ source('quality', 'source_freshness') }}

),

renamed as (

    select
        invocation_id,
        run_started_at,
        source_table,
        max_recorded_at,
        max_sequence,
        lag_seconds,
        recorded_at
    from source

)

select * from renamed
