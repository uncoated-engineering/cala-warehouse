{{ config(tags=['quality']) }}

-- grain: one row per (invocation_id, model_name)
--
-- Row count of every model per build, against the previous build that
-- counted the same model. A jump beyond var('row_count_drift_threshold_pct')
-- is flagged, not failed: a backfill and a broken join look the same here,
-- and telling them apart is the reader's job.
with counts as (

    select * from {{ ref('stg_quality_row_counts') }}

),

with_previous as (

    select
        invocation_id,
        run_started_at,
        model_name,
        relation_name,
        row_count,
        lag(row_count) over (
            partition by model_name order by run_started_at, invocation_id
        )                                                       as previous_row_count,
        lag(invocation_id) over (
            partition by model_name order by run_started_at, invocation_id
        )                                                       as previous_invocation_id
    from counts

),

final as (

    select
        invocation_id,
        run_started_at,
        model_name,
        relation_name,
        row_count,
        previous_row_count,
        previous_invocation_id,
        row_count - previous_row_count                          as delta,
        case
            when previous_row_count > 0
                then 100.0 * (row_count - previous_row_count) / previous_row_count
        end                                                     as drift_pct,
        coalesce(
            abs(100.0 * (row_count - previous_row_count) / nullif(previous_row_count, 0))
                > {{ var('row_count_drift_threshold_pct') }},
            false
        )                                                       as is_outlier
    from with_previous

)

select * from final
