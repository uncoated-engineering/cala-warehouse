{{ config(tags=['quality']) }}

-- grain: one row per invocation_id (one dbt run)
--
-- The three quality KPIs tracked over time, one row per dbt invocation:
--   * test pass rate, and separately whether every accounting control passed
--   * freshness: how far behind the invocation's start the newest entry and
--     the newest outbox row were
--   * row-count drift: the largest jump of any model against its previous
--     build, and how many models crossed the threshold
-- An invocation's own results are recorded after it ends, so this table
-- describes previous runs; `make quality` refreshes it without re-testing.
with tests as (

    select * from {{ ref('fct_test_results') }}

),

drift as (

    select * from {{ ref('fct_row_count_drift') }}

),

freshness as (

    select * from {{ ref('stg_quality_freshness') }}

),

invocations as (

    select invocation_id, run_started_at from tests
    union
    select invocation_id, run_started_at from drift
    union
    select invocation_id, run_started_at from freshness

),

test_kpis as (

    select
        invocation_id,
        count(*)                                                as tests_run,
        {{ count_where('passed') }}                             as tests_passed,
        {{ count_where("status = 'fail'") }}                    as tests_failed,
        {{ count_where("status = 'error'") }}                   as tests_errored,
        {{ count_where('is_control') }}                         as controls_run,
        {{ count_where('is_control and passed') }}              as controls_passed
    from tests
    group by 1

),

drift_kpis as (

    select
        invocation_id,
        count(*)                                                as models_counted,
        sum(row_count)                                          as rows_total,
        max(abs(drift_pct))                                     as max_abs_drift_pct,
        {{ count_where('is_outlier') }}                         as outlier_models
    from drift
    group by 1

),

freshness_kpis as (

    select
        invocation_id,
        max(case when source_table = 'cala_entry_events' then lag_seconds end)
                                                                as entries_freshness_lag_s,
        max(case when source_table = 'cala_persistent_outbox_events' then lag_seconds end)
                                                                as outbox_freshness_lag_s,
        max(case when source_table = 'cala_persistent_outbox_events' then max_sequence end)
                                                                as outbox_max_sequence
    from freshness
    group by 1

),

final as (

    select
        i.invocation_id,
        i.run_started_at,
        coalesce(t.tests_run, 0)                                as tests_run,
        coalesce(t.tests_passed, 0)                             as tests_passed,
        coalesce(t.tests_failed, 0)                             as tests_failed,
        coalesce(t.tests_errored, 0)                            as tests_errored,
        case
            when t.tests_run > 0 then 1.0 * t.tests_passed / t.tests_run
        end                                                     as pass_rate,
        coalesce(t.controls_run, 0)                             as controls_run,
        coalesce(t.controls_passed, 0)                          as controls_passed,
        case
            when t.controls_run > 0 then t.controls_passed = t.controls_run
        end                                                     as all_controls_passed,
        coalesce(d.models_counted, 0)                           as models_counted,
        d.rows_total,
        d.max_abs_drift_pct,
        coalesce(d.outlier_models, 0)                           as outlier_models,
        f.entries_freshness_lag_s,
        f.outbox_freshness_lag_s,
        f.outbox_max_sequence
    from invocations as i
    left join test_kpis as t
        on t.invocation_id = i.invocation_id
    left join drift_kpis as d
        on d.invocation_id = i.invocation_id
    left join freshness_kpis as f
        on f.invocation_id = i.invocation_id

)

select * from final
