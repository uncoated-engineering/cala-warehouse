{#-
  A source table the WAREHOUSE writes, not cala. `raw.cala_erasure_log` is
  appended by cala-erase / cala-extract, so on a warehouse that has never
  erased anything it does not exist yet. Models still have to build, so this
  resolves to the real relation when it is there and to an empty, typed,
  inline relation when it is not. The source() call is made unconditionally
  so dbt records the dependency either way.

  `columns` is a list of (name, type) pairs. The empty branch is a plain
  `select ... where 1 = 0` with no FROM, valid on DuckDB and BigQuery.
-#}
{% macro optional_source(source_name, table_name, columns) -%}
    {%- set rel = source(source_name, table_name) -%}
    {%- if execute and load_relation(rel) is none -%}
    (
        select
            {%- for name, type in columns %}
            cast(null as {{ type }}) as {{ name }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        where 1 = 0
    )
    {%- else -%}
    {{ rel }}
    {%- endif -%}
{%- endmacro %}

{#- The shape of raw.cala_erasure_log as extract/cala_extract/erasure.py writes it. -#}
{% macro erasure_log_columns() -%}
    {{ return([
        ('log_id',        dbt.type_string()),
        ('erasure_id',    dbt.type_string()),
        ('applied_at',    dbt.type_timestamp()),
        ('run_id',        dbt.type_string()),
        ('source',        dbt.type_string()),
        ('requested_by',  dbt.type_string()),
        ('reason',        dbt.type_string()),
        ('entity_kind',   dbt.type_string()),
        ('entity_id',     dbt.type_string()),
        ('cascade_of',    dbt.type_string()),
        ('table_name',    dbt.type_string()),
        ('fields',        dbt.type_string()),
        ('rows_matched',  dbt.type_bigint()),
        ('rows_redacted', dbt.type_bigint()),
    ]) }}
{%- endmacro %}
