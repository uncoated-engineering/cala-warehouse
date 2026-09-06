{#-
  Cross-database helpers. Models call these instead of dialect functions so
  the same SQL compiles on DuckDB (local/CI) and BigQuery (lana's warehouse).

  JSON columns arrive as strings (see seeds config). Paths are JSONPath
  without the leading "$." — e.g. json_string(event, 'values.units').
-#}

{% macro decimal_type() -%}
  {{ return(adapter.dispatch('decimal_type', 'cala_warehouse')()) }}
{%- endmacro %}
{% macro default__decimal_type() -%} decimal(38, 18) {%- endmacro %}
{% macro bigquery__decimal_type() -%} bignumeric {%- endmacro %}


{% macro json_string(column, path) -%}
  {{ return(adapter.dispatch('json_string', 'cala_warehouse')(column, path)) }}
{%- endmacro %}
{% macro default__json_string(column, path) -%}
  json_extract_string({{ column }}, '$.{{ path }}')
{%- endmacro %}
{% macro bigquery__json_string(column, path) -%}
  json_value({{ column }}, '$.{{ path }}')
{%- endmacro %}


{% macro json_decimal(column, path) -%}
  cast({{ json_string(column, path) }} as {{ decimal_type() }})
{%- endmacro %}


{% macro json_int(column, path) -%}
  cast({{ json_string(column, path) }} as integer)
{%- endmacro %}


{% macro json_bool(column, path) -%}
  cast({{ json_string(column, path) }} as boolean)
{%- endmacro %}


{% macro json_timestamp(column, path) -%}
  cast({{ json_string(column, path) }} as timestamp)
{%- endmacro %}


{#- Keep a nested object as a JSON string (for metadata / context passthrough). -#}
{% macro json_object(column, path) -%}
  {{ return(adapter.dispatch('json_object', 'cala_warehouse')(column, path)) }}
{%- endmacro %}
{% macro default__json_object(column, path) -%}
  cast(json_extract({{ column }}, '$.{{ path }}') as varchar)
{%- endmacro %}
{% macro bigquery__json_object(column, path) -%}
  json_query({{ column }}, '$.{{ path }}')
{%- endmacro %}


{#- Extract a JSON array of strings as a native array. -#}
{% macro json_string_array(column, path) -%}
  {{ return(adapter.dispatch('json_string_array', 'cala_warehouse')(column, path)) }}
{%- endmacro %}
{% macro default__json_string_array(column, path) -%}
  cast(json_extract({{ column }}, '$.{{ path }}') as varchar[])
{%- endmacro %}
{% macro bigquery__json_string_array(column, path) -%}
  json_value_array({{ column }}, '$.{{ path }}')
{%- endmacro %}


{#- Does a native string array contain a value? -#}
{% macro array_has(array_expr, value) -%}
  {{ return(adapter.dispatch('array_has', 'cala_warehouse')(array_expr, value)) }}
{%- endmacro %}
{% macro default__array_has(array_expr, value) -%}
  list_contains({{ array_expr }}, {{ value }})
{%- endmacro %}
{% macro bigquery__array_has(array_expr, value) -%}
  {{ value }} in unnest({{ array_expr }})
{%- endmacro %}


{#- count(*) filter (where ...) is DuckDB/Postgres; BigQuery has countif. -#}
{% macro count_where(condition) -%}
  {{ return(adapter.dispatch('count_where', 'cala_warehouse')(condition)) }}
{%- endmacro %}
{% macro default__count_where(condition) -%}
  count(*) filter (where {{ condition }})
{%- endmacro %}
{% macro bigquery__count_where(condition) -%}
  countif({{ condition }})
{%- endmacro %}


{#- Aggregate AND over a boolean column. -#}
{% macro bool_and_agg(expression) -%}
  {{ return(adapter.dispatch('bool_and_agg', 'cala_warehouse')(expression)) }}
{%- endmacro %}
{% macro default__bool_and_agg(expression) -%}
  bool_and({{ expression }})
{%- endmacro %}
{% macro bigquery__bool_and_agg(expression) -%}
  logical_and({{ expression }})
{%- endmacro %}
