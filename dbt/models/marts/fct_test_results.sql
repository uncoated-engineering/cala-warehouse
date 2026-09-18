{{ config(tags=['quality']) }}

-- grain: one row per (invocation_id, test_unique_id)
--
-- Every test result dbt has recorded, with the two flags the KPIs need:
-- whether the test is one of the accounting controls (a singular test in
-- dbt/tests/) and whether it passed. Rows describe a past run; nothing here
-- gates a build.
with results as (

    select * from {{ ref('stg_quality_test_results') }}

),

final as (

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
        test_kind = 'singular'                                  as is_control,
        status = 'pass'                                         as passed,
        recorded_at
    from results

)

select * from final
