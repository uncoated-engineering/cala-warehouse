-- Reconciliation control: our balances, rebuilt from raw entries, must equal
-- the balances cala itself persisted.
--
-- Two independent computations of the same number from the same source of
-- truth: cala accumulates balances at posting time inside the ledger
-- transaction; fct_account_balances re-sums fct_entries in the warehouse.
-- Agreement on every row is strong evidence that both the ledger and the
-- warehouse copy of its entries are correct. A disagreement is a real bug in
-- one of the two, or a partial load.
--
-- Rules of the comparison:
--   * full grain (journal_id, account_id, currency, layer); never net across layers
--   * debits and credits compared separately, as exact decimals, zero tolerance
--   * a layer cala stores as 0/0 that we have no entries for is a match, not a
--     miss (cala writes all three layers on first touch of an account/currency)
--   * any grain present on one side with a non-zero amount and absent on the
--     other is a failure
--   * as-of: both sides come from the same dump, so both are "now"; a
--     production run must compare against a balance snapshot taken at the same
--     watermark as the entries
--   * account-set backing accounts are excluded here: no entry ever posts to
--     them, their balances are rollups. See assert_account_set_balances_reconcile.
with accounts as (

    select
        account_id,
        is_account_set
    from {{ ref('stg_cala_accounts') }}
    qualify row_number() over (partition by account_id order by sequence desc) = 1

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
    inner join accounts as a
        on a.account_id = b.account_id
    where not a.is_account_set

),

warehouse as (

    select
        journal_id,
        account_id,
        currency,
        layer,
        dr_balance,
        cr_balance
    from {{ ref('fct_account_balances') }}

),

compared as (

    select
        coalesce(c.journal_id, w.journal_id)                    as journal_id,
        coalesce(c.account_id, w.account_id)                    as account_id,
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
    full outer join warehouse as w
        on  w.journal_id = c.journal_id
        and w.account_id = c.account_id
        and w.currency   = c.currency
        and w.layer      = c.layer

)

select *
from compared
where status not in ('ok', 'ok_untouched_layer')
