-- Reconciliation control, hierarchy edition: cala's balance for an ACCOUNT SET
-- must equal the sum of our recomputed balances over every account underneath
-- it, at any depth.
--
-- No entry ever posts to a set; cala maintains set balances by rolling member
-- balances up the hierarchy at posting time. That rollup is exactly what
-- dim_account_set_members (the recursive closure) lets us rebuild in the
-- warehouse from first principles: leaf balances joined to the closure and
-- summed per ancestor set. So this test checks cala's graph walk and its
-- rollup arithmetic against ours.
--
-- Two cala behaviours shape the comparison:
--   * an account may reach a set by several paths (DAG); it must be counted
--     once per set, so the closure is de-duplicated on (set, account) first
--   * a set belongs to ONE journal, but an account does not: the same account
--     can be a member of sets in different journals. cala rolls a posting up
--     only into ancestor sets that live in the posting's journal (see cala's
--     test `a_multi_journal_batch_does_not_cross_ancestor_sets_between_journals`),
--     so the rollup here is scoped to the set's journal. Without that filter
--     this test reports phantom balances for the cross-journal sets.
with closure as (

    select distinct
        m.account_set_id,
        s.journal_id,
        m.member_id                                             as account_id
    from {{ ref('dim_account_set_members') }} as m
    inner join {{ ref('dim_account_sets') }} as s
        on s.account_set_id = m.account_set_id
    where m.member_kind = 'account'

),

rolled_up as (

    select
        b.journal_id,
        c.account_set_id                                        as account_id,
        b.currency,
        b.layer,
        sum(b.dr_balance)                                       as dr_balance,
        sum(b.cr_balance)                                       as cr_balance
    from {{ ref('fct_account_balances') }} as b
    inner join closure as c
        on  c.account_id = b.account_id
        and c.journal_id = b.journal_id
    group by 1, 2, 3, 4

),

cala as (

    select
        b.journal_id,
        b.account_id,
        b.currency,
        b.layer,
        b.dr_balance,
        b.cr_balance
    from {{ ref('stg_cala_balances') }} as b
    inner join {{ ref('dim_accounts') }} as a
        on  a.account_id = b.account_id
        and a.is_current
    where a.is_account_set

),

compared as (

    select
        coalesce(c.journal_id, w.journal_id)                    as journal_id,
        coalesce(c.account_id, w.account_id)                    as account_set_id,
        coalesce(c.currency, w.currency)                        as currency,
        coalesce(c.layer, w.layer)                              as layer,
        c.dr_balance                                            as cala_dr_balance,
        c.cr_balance                                            as cala_cr_balance,
        w.dr_balance                                            as warehouse_dr_balance,
        w.cr_balance                                            as warehouse_cr_balance,
        case
            when c.account_id is null then 'missing_in_cala'
            when w.account_id is null and (c.dr_balance <> 0 or c.cr_balance <> 0)
                                      then 'missing_in_warehouse'
            when w.account_id is null then 'ok_untouched_layer'
            when c.dr_balance <> w.dr_balance
              or c.cr_balance <> w.cr_balance
                                      then 'amount_mismatch'
            else 'ok'
        end                                                     as status
    from cala as c
    full outer join rolled_up as w
        on  w.journal_id = c.journal_id
        and w.account_id = c.account_id
        and w.currency   = c.currency
        and w.layer      = c.layer

)

select *
from compared
where status not in ('ok', 'ok_untouched_layer')
