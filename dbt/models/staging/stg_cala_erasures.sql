-- grain: one row per erasure log row (log_id)
--
-- The erasure audit log the extractor keeps (extract/cala_extract/erasure.py):
-- what was requested, what was redacted where, and every later run that had
-- to redact the same entity again because upstream re-landed the value.
-- Read through optional_source: a warehouse that has never erased anything
-- has no log table yet, and every model downstream must still build.
with source as (

    select * from {{ optional_source('cala', 'cala_erasure_log', erasure_log_columns()) }}

),

renamed as (

    select
        log_id,
        erasure_id,
        applied_at,
        run_id,
        source                                                  as erasure_source,
        requested_by,
        reason,
        entity_kind,
        entity_id,
        cascade_of,
        table_name,
        fields,
        rows_matched,
        rows_redacted
    from source

)

select * from renamed
