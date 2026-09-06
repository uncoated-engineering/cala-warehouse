-- grain: one row per membership change (outbox sequence)
--
-- Membership add/remove is not an entity event in cala: the `*_events`
-- streams for account sets only carry initialized/updated. The change is
-- published on the outbox as AccountSetMemberCreated / AccountSetMemberRemoved,
-- so that is the source. `member_id` is serde-tagged:
--   {"type": "Account", "id": ...} | {"type": "AccountSet", "id": ...}
with source as (

    select * from {{ source('cala', 'cala_persistent_outbox_events') }}

),

filtered as (

    select
        sequence                                                as outbox_sequence,
        {{ json_string('payload', 'type') }}                    as payload_type,
        {{ json_string('payload', 'account_set_id') }}          as account_set_id,
        {{ json_string('payload', 'member_id.id') }}            as member_id,
        {{ json_string('payload', 'member_id.type') }}          as member_kind,
        recorded_at,
        tracing_context                                         as event_context
    from source
    where {{ json_string('payload', 'type') }}
          in ('account_set_member_created', 'account_set_member_removed')

),

renamed as (

    select
        outbox_sequence,
        account_set_id,
        member_id,
        case member_kind
            when 'Account'    then 'account'
            when 'AccountSet' then 'account_set'
        end                                                     as member_kind,
        case payload_type
            when 'account_set_member_created' then 'added'
            when 'account_set_member_removed' then 'removed'
        end                                                     as membership_change,
        recorded_at,
        event_context
    from filtered

)

select * from renamed
