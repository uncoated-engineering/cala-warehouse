-- grain: one row per account set (current state)
--
-- Current attributes from the latest event, plus where the set sits in the
-- hierarchy: its direct parents/children and its depth from a root. Depth
-- and ancestry come from dim_account_set_members (the recursive closure);
-- this model is the flat, one-row-per-set view of the same structure.
with sets as (

    select * from {{ ref('stg_cala_account_sets') }}
    qualify row_number() over (partition by account_set_id order by sequence desc) = 1

),

membership as (

    select * from {{ ref('dim_account_set_members') }}

),

direct_children as (

    select
        account_set_id,
        count(*) filter (where member_kind = 'account')         as direct_account_count,
        count(*) filter (where member_kind = 'account_set')     as direct_set_count
    from membership
    where depth = 1
    group by 1

),

descendants as (

    select
        account_set_id,
        count(distinct member_id) filter (where member_kind = 'account')
                                                                as descendant_account_count,
        max(depth)                                              as max_depth
    from membership
    group by 1

),

-- A set's depth is its longest path up to a root. A set that is a member of
-- nothing is a root (depth 0).
parents as (

    select
        member_id                                               as account_set_id,
        max(depth)                                              as depth_from_root,
        count(distinct account_set_id) filter (where depth = 1) as direct_parent_count
    from membership
    where member_kind = 'account_set'
    group by 1

),

final as (

    select
        s.account_set_id,
        s.journal_id,
        s.name,
        s.external_id,
        s.description,
        s.normal_balance_type,
        s.metadata,
        s.version                                               as cala_version,
        coalesce(p.depth_from_root, 0)                          as depth_from_root,
        coalesce(p.direct_parent_count, 0) = 0                  as is_root,
        coalesce(p.direct_parent_count, 0)                      as direct_parent_count,
        coalesce(c.direct_account_count, 0)                     as direct_account_count,
        coalesce(c.direct_set_count, 0)                         as direct_set_count,
        coalesce(d.descendant_account_count, 0)                 as descendant_account_count,
        coalesce(d.max_depth, 0)                                as subtree_depth,
        s.recorded_at                                           as last_event_at,
        s.event_context
    from sets as s
    left join direct_children as c on c.account_set_id = s.account_set_id
    left join descendants as d     on d.account_set_id = s.account_set_id
    left join parents as p         on p.account_set_id = s.account_set_id

)

select * from final
