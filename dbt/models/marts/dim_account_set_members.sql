-- grain: one row per (account_set_id, member_id, path)  — transitive closure
--
-- Which accounts (and sets) are inside which set, at any depth, as of now.
--
-- Step 1 replays AccountSetMemberCreated / AccountSetMemberRemoved from the
-- outbox to get current DIRECT edges: the last change per (set, member) wins.
-- Step 2 walks set->set edges recursively so every set sees every account
-- underneath it. Members can be accounts or other sets, and a set may belong
-- to several parents, so this is a DAG, not a tree: an account can reach the
-- same ancestor by more than one path, which is why `path` is part of the
-- grain. The walk refuses to re-enter a set already on its path, so a cycle
-- in the source terminates instead of recursing forever.
with recursive changes as (

    select * from {{ ref('stg_cala_account_set_members') }}

),

current_edges as (

    select
        account_set_id,
        member_id,
        member_kind,
        recorded_at                                             as member_since
    from changes
    qualify row_number() over (
        partition by account_set_id, member_id
        order by outbox_sequence desc
    ) = 1
    -- keep only edges whose most recent change was an add
    and membership_change = 'added'

),

closure as (

    -- depth 1: direct members
    select
        account_set_id,
        member_id,
        member_kind,
        1                                                       as depth,
        account_set_id || '>' || member_id                      as path,
        member_since
    from current_edges

    union all

    -- depth n+1: members of member sets
    select
        c.account_set_id,
        e.member_id,
        e.member_kind,
        c.depth + 1                                             as depth,
        c.path || '>' || e.member_id                            as path,
        greatest(c.member_since, e.member_since)                as member_since
    from closure as c
    inner join current_edges as e
        on  e.account_set_id = c.member_id
        and c.member_kind = 'account_set'
    where strpos(c.path, e.member_id) = 0   -- cycle guard

)

select
    account_set_id,
    member_id,
    member_kind,
    depth,
    path,
    member_since
from closure
