{{ config(tags=['quality']) }}

-- grain: one row per (invocation_id, model_name)
--
-- count(*) of every model the invocation built successfully, taken by the
-- on-run-end hook right after the run. The input to row-count drift.
with source as (

    select * from {{ source('quality', 'model_row_counts') }}

),

renamed as (

    select
        invocation_id,
        run_started_at,
        model_name,
        relation_name,
        row_count,
        recorded_at
    from source

)

select * from renamed
