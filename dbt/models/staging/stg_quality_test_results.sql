{{ config(tags=['quality']) }}

-- grain: one row per (invocation_id, test_unique_id)
--
-- What the on-run-end hook (macros/quality.sql) recorded for every test
-- dbt ran: its status, how many rows it returned, how long it took.
with source as (

    select * from {{ source('quality', 'dbt_test_results') }}

),

renamed as (

    select
        invocation_id,
        run_started_at,
        test_unique_id,
        test_name,
        test_kind,
        generic_test_name,
        tested_model,
        status,
        failures,
        execution_time_s,
        message,
        recorded_at
    from source

)

select * from renamed
