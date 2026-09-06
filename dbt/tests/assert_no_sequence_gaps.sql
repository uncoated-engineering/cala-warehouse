-- Completeness control: for every entity, the event stream has no gaps.
--
-- es-entity assigns `sequence` 1, 2, 3, ... per entity id. If the warehouse
-- copy of a stream is complete, then for each id
--     min(sequence) = 1  and  max(sequence) = count(*)
-- (with UNIQUE(id, sequence) already guaranteed upstream, count = max implies
-- contiguity). A row here means events were lost between Postgres and the
-- warehouse, which is exactly the kind of thing a SOC 2 completeness control
-- is meant to catch. It covers every stream we stage, in one place.
{% set streams = [
    ('stg_cala_journals',      'journal_id'),
    ('stg_cala_accounts',      'account_id'),
    ('stg_cala_account_sets',  'account_set_id'),
    ('stg_cala_transactions',  'transaction_id'),
    ('stg_cala_entries',       'entry_id'),
] %}

with events as (

    {% for model, id_column in streams %}
    select
        '{{ model }}'                                           as stream,
        {{ id_column }}                                         as entity_id,
        sequence
    from {{ ref(model) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select
    stream,
    entity_id,
    min(sequence)                                               as min_sequence,
    max(sequence)                                               as max_sequence,
    count(*)                                                    as event_count
from events
group by 1, 2
having min(sequence) <> 1
    or max(sequence) <> count(*)
