-- Erasure control: an entity in the erasure log holds no personal data
-- anywhere in the warehouse.
--
-- Erasure is applied to the raw tables by extract/cala_extract/erasure.py
-- (targeted deletion: the personal fields are set to null, the accounting
-- columns are never touched). This test is the warehouse-side counterpart of
-- es-entity's `verify_forgotten`: it does not trust the log's own counts, it
-- looks at every place a value could survive:
--
--   * the staging views (they read raw, so a value here is a value in raw)
--   * the marts that carry the fields (tables: a value here means the mart
--     was built before the erasure and not re-materialised since)
--   * the raw state tables the extractor REPLACES every run (a value here
--     means a run landed the source's copy and did not re-apply the log)
--   * the raw outbox payloads, which carry a copy of the entity's values
--
-- The field list mirrors the policy in erasure.py; keep the two together.
-- Account and account set share an id, so an erasure of either kind is
-- checked against both. An empty string is a value. A failure names the
-- relation, the entity and the field that still holds something; the fix
-- is `cala-erase reapply` and a rebuild, never loosening this test.
{% set personal = ['name', 'description', 'external_id', 'metadata'] %}
{% set account_kinds = "('account', 'account_set')" %}

{#- (relation sql, entity_kind filter, id column, fields) -#}
{% set holders = [
    (ref('stg_cala_accounts'),       account_kinds,      'account_id',     personal),
    (ref('stg_cala_account_sets'),   account_kinds,      'account_set_id', personal),
    (ref('stg_cala_transactions'),   "('transaction')",  'transaction_id', ['description', 'external_id', 'metadata']),
    (ref('stg_cala_entries'),        "('entry')",        'entry_id',       ['description', 'metadata']),
    (ref('stg_cala_journals'),       "('journal')",      'journal_id',     ['name', 'description']),
    (ref('dim_accounts'),            account_kinds,      'account_id',     personal),
    (ref('dim_account_sets'),        account_kinds,      'account_set_id', personal),
    (ref('fct_entries'),             "('entry')",        'entry_id',       ['description', 'metadata']),
    (ref('fct_transactions'),        "('transaction')",  'transaction_id', ['description', 'external_id', 'metadata']),
    (source('cala', 'cala_accounts'),     account_kinds,     'id', ['name', 'external_id']),
    (source('cala', 'cala_account_sets'), account_kinds,     'id', ['name', 'external_id']),
    (source('cala', 'cala_transactions'), "('transaction')", 'id', ['external_id']),
    (source('cala', 'cala_journals'),     "('journal')",     'id', ['name']),
] %}

{#- outbox payload key -> (entity_kind filter, fields under that key) -#}
{% set outbox = [
    ('account',     account_kinds,     personal),
    ('account_set', account_kinds,     personal),
    ('transaction', "('transaction')", ['description', 'external_id', 'metadata']),
    ('entry',       "('entry')",       ['description', 'metadata']),
    ('journal',     "('journal')",     ['name', 'description']),
] %}

with erased as (

    select distinct
        entity_kind,
        entity_id
    from {{ ref('stg_cala_erasures') }}

),

survivors as (

    {% for relation, kinds, id_column, fields in holders %}
    {% for field in fields %}
    select
        '{{ relation.identifier }}'                             as relation,
        e.entity_kind,
        e.entity_id,
        '{{ field }}'                                           as field
    from {{ relation }} as h
    inner join erased as e
        on e.entity_id = h.{{ id_column }}
       and e.entity_kind in {{ kinds }}
    where h.{{ field }} is not null

    union all
    {% endfor %}
    {% endfor %}

    {% for key, kinds, fields in outbox %}
    {% for field in fields %}
    select
        'cala_persistent_outbox_events'                         as relation,
        e.entity_kind,
        e.entity_id,
        '{{ key }}.{{ field }}'                                 as field
    from {{ source('cala', 'cala_persistent_outbox_events') }} as o
    inner join erased as e
        on e.entity_id = {{ json_string('o.payload', key ~ '.id') }}
       and e.entity_kind in {{ kinds }}
    where {{ json_string('o.payload', key ~ '.' ~ field) }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select
    relation,
    entity_kind,
    entity_id,
    field
from survivors
