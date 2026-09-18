-- grain: one row per erasure request (erasure_id)
--
-- The auditor's view of the log: what was asked, by whom, what it touched,
-- and how many later extraction runs had to redact the same entity again
-- because the source still holds the value (cala has no forgettable fields,
-- so a replaced state table brings the name back until the run re-applies
-- the erasure). A request row is one of operator | forget_event | cascade;
-- reapply rows are folded into reapply_count / last_reapplied_at.
with log as (

    select * from {{ ref('stg_cala_erasures') }}

),

requests as (

    select
        erasure_id,
        entity_kind,
        entity_id,
        erasure_source,
        requested_by,
        reason,
        cascade_of,
        run_id,
        applied_at
    from log
    where erasure_source <> 'reapply'
    qualify row_number() over (partition by erasure_id order by applied_at, log_id) = 1

),

per_request as (

    select
        erasure_id,
        min(applied_at)                                         as first_applied_at,
        count(distinct table_name)                              as tables_touched,
        sum(rows_redacted)                                      as rows_redacted_total,
        {{ count_where("erasure_source = 'reapply'") }}         as reapply_count,
        max(case when erasure_source = 'reapply' then applied_at end)
                                                                as last_reapplied_at
    from log
    group by 1

),

final as (

    select
        r.erasure_id,
        r.entity_kind,
        r.entity_id,
        r.erasure_source,
        r.requested_by,
        r.reason,
        r.cascade_of,
        r.run_id,
        p.first_applied_at,
        p.tables_touched,
        p.rows_redacted_total,
        p.reapply_count,
        p.last_reapplied_at
    from requests as r
    inner join per_request as p
        on p.erasure_id = r.erasure_id

)

select * from final
