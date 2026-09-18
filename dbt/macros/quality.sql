{#-
  The quality layer's recorder. Two hooks, wired in dbt_project.yml:

    on-run-start  quality_ensure_tables()          create quality.* if missing
    on-run-end    quality_record_results(results)  append what this invocation did

  Every dbt invocation (build, run, test) then leaves three kinds of facts
  behind, keyed by invocation_id: one row per test result, one row per model
  built with its row count, and the freshness of the two source streams that
  drive everything else. The models under models/staging/stg_quality_* and
  the `quality`-tagged marts turn those rows into KPIs tracked over time.

  Rules: the hooks record, they never gate. Nothing here raises on a failed
  test; the build's own exit code already carries that. Inserts are batched
  (one statement per table) so the cost on BigQuery is one small job each.
-#}

{% macro quality_relation(identifier) -%}
  {{ return(api.Relation.create(database=target.database, schema='quality', identifier=identifier)) }}
{%- endmacro %}


{% macro quality_ensure_tables() %}
  {% if execute %}
    {% do adapter.create_schema(quality_relation('dbt_test_results')) %}
    {% call statement('quality_ensure_test_results') %}
      create table if not exists {{ quality_relation('dbt_test_results') }} (
          invocation_id     {{ dbt.type_string() }},
          run_started_at    {{ dbt.type_timestamp() }},
          test_unique_id    {{ dbt.type_string() }},
          test_name         {{ dbt.type_string() }},
          test_kind         {{ dbt.type_string() }},
          generic_test_name {{ dbt.type_string() }},
          tested_model      {{ dbt.type_string() }},
          status            {{ dbt.type_string() }},
          failures          {{ dbt.type_bigint() }},
          execution_time_s  {{ dbt.type_float() }},
          message           {{ dbt.type_string() }},
          recorded_at       {{ dbt.type_timestamp() }}
      )
    {% endcall %}
    {% call statement('quality_ensure_row_counts') %}
      create table if not exists {{ quality_relation('model_row_counts') }} (
          invocation_id  {{ dbt.type_string() }},
          run_started_at {{ dbt.type_timestamp() }},
          model_name     {{ dbt.type_string() }},
          relation_name  {{ dbt.type_string() }},
          row_count      {{ dbt.type_bigint() }},
          recorded_at    {{ dbt.type_timestamp() }}
      )
    {% endcall %}
    {% call statement('quality_ensure_freshness') %}
      create table if not exists {{ quality_relation('source_freshness') }} (
          invocation_id   {{ dbt.type_string() }},
          run_started_at  {{ dbt.type_timestamp() }},
          source_table    {{ dbt.type_string() }},
          max_recorded_at {{ dbt.type_timestamp() }},
          max_sequence    {{ dbt.type_bigint() }},
          lag_seconds     {{ dbt.type_float() }},
          recorded_at     {{ dbt.type_timestamp() }}
      )
    {% endcall %}
    {# statement() opens a transaction; a hook whose rendered SQL is empty
       never commits it, so commit here. #}
    {% do adapter.commit() %}
  {% endif %}
{% endmacro %}


{#- SQL literal helpers. Strings: quotes doubled, backslashes dropped (BigQuery
    treats them as escapes), length capped. Timestamps: naive UTC text cast to
    the adapter's timestamp type. -#}
{% macro quality_str(value, max_len=1000) -%}
  {%- if value is none -%}
    cast(null as {{ dbt.type_string() }})
  {%- else -%}
    '{{ (value | string)[:max_len] | replace("\\", "/") | replace("'", "''") }}'
  {%- endif -%}
{%- endmacro %}

{% macro quality_ts(value) -%}
  {%- if value is none -%}
    cast(null as {{ dbt.type_timestamp() }})
  {%- else -%}
    cast('{{ value }}' as {{ dbt.type_timestamp() }})
  {%- endif -%}
{%- endmacro %}

{% macro quality_num(value, type) -%}
  {%- if value is none -%}
    cast(null as {{ type }})
  {%- else -%}
    cast({{ value }} as {{ type }})
  {%- endif -%}
{%- endmacro %}

{% macro quality_utc_text(datetime_value) -%}
  {{ return(datetime_value.astimezone(modules.pytz.utc).strftime('%Y-%m-%d %H:%M:%S.%f')) }}
{%- endmacro %}

{#- The model or source a test is attached to, by name. Generic tests carry
    `attached_node`; singular tests only their depends_on list. -#}
{% macro quality_tested_model(node) -%}
  {%- set attached = node.attached_node if node.attached_node is defined else none -%}
  {%- if attached -%}
    {{ return(attached.split('.')[-1]) }}
  {%- endif -%}
  {%- for dep in node.depends_on.nodes -%}
    {%- if dep.startswith('model.') or dep.startswith('source.') -%}
      {{ return(dep.split('.')[-1]) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(none) }}
{%- endmacro %}


{% macro quality_record_results(results) %}
  {% if execute %}
    {% set started = quality_utc_text(run_started_at) %}
    {% set now = modules.datetime.datetime.now(modules.pytz.utc).strftime('%Y-%m-%d %H:%M:%S.%f') %}

    {# --- tests --------------------------------------------------------- #}
    {% set test_rows = [] %}
    {% for result in results if result.node.resource_type == 'test' %}
      {% set node = result.node %}
      {% set is_generic = node.test_metadata is defined and node.test_metadata %}
      {% do test_rows.append(
        "(" ~ quality_str(invocation_id) ~ ", " ~ quality_ts(started) ~ ", "
        ~ quality_str(node.unique_id) ~ ", " ~ quality_str(node.name) ~ ", "
        ~ quality_str('generic' if is_generic else 'singular') ~ ", "
        ~ quality_str(node.test_metadata.name if is_generic else none) ~ ", "
        ~ quality_str(quality_tested_model(node)) ~ ", "
        ~ quality_str(result.status | string) ~ ", "
        ~ quality_num(result.failures, dbt.type_bigint()) ~ ", "
        ~ quality_num(result.execution_time, dbt.type_float()) ~ ", "
        ~ quality_str(result.message) ~ ", " ~ quality_ts(now) ~ ")"
      ) %}
    {% endfor %}
    {% if test_rows %}
      {% call statement('quality_insert_test_results') %}
        insert into {{ quality_relation('dbt_test_results') }}
          (invocation_id, run_started_at, test_unique_id, test_name, test_kind, generic_test_name,
           tested_model, status, failures, execution_time_s, message, recorded_at)
        values {{ test_rows | join(',\n               ') }}
      {% endcall %}
    {% endif %}

    {# --- row counts ---------------------------------------------------- #}
    {# The quality marts themselves are skipped: they grow by construction
       on every run and would flag their own drift. #}
    {% set count_rows = [] %}
    {% for result in results
       if result.node.resource_type == 'model'
       and (result.status | string) == 'success'
       and result.node.config.materialized != 'ephemeral'
       and 'quality' not in result.node.tags %}
      {% set node = result.node %}
      {% set counted = run_query('select count(*) as n from ' ~ node.relation_name) %}
      {% set n = counted.rows[0][0] if counted and counted.rows else none %}
      {% do count_rows.append(
        "(" ~ quality_str(invocation_id) ~ ", " ~ quality_ts(started) ~ ", "
        ~ quality_str(node.name) ~ ", " ~ quality_str(node.relation_name) ~ ", "
        ~ quality_num(n, dbt.type_bigint()) ~ ", " ~ quality_ts(now) ~ ")"
      ) %}
    {% endfor %}
    {% if count_rows %}
      {% call statement('quality_insert_row_counts') %}
        insert into {{ quality_relation('model_row_counts') }}
          (invocation_id, run_started_at, model_name, relation_name, row_count, recorded_at)
        values {{ count_rows | join(',\n               ') }}
      {% endcall %}
    {% endif %}

    {# --- freshness ----------------------------------------------------- #}
    {# Only for invocations that built or tested something: a bare `dbt seed`
       would otherwise leave an invocation with freshness and nothing else. #}
    {% if test_rows or count_rows %}
      {% set fresh_rows = [] %}
      {% for source_table, seq_col in [('cala_entry_events', none), ('cala_persistent_outbox_events', 'sequence')] %}
        {% set rel = source('cala', source_table) %}
        {% if load_relation(rel) is not none %}
          {% set seq_expr = 'max(' ~ seq_col ~ ')' if seq_col else 'null' %}
          {% set fresh = run_query(
            'select max(recorded_at), ' ~ seq_expr ~ ', '
            ~ seconds_between('max(recorded_at)', quality_ts(started)) ~ ' from ' ~ rel
          ) %}
          {% set row = fresh.rows[0] if fresh and fresh.rows else none %}
          {% if row %}
            {% do fresh_rows.append(
              "(" ~ quality_str(invocation_id) ~ ", " ~ quality_ts(started) ~ ", "
              ~ quality_str(source_table) ~ ", "
              ~ quality_ts(row[0].strftime('%Y-%m-%d %H:%M:%S.%f') if row[0] is not none else none) ~ ", "
              ~ quality_num(row[1], dbt.type_bigint()) ~ ", "
              ~ quality_num(row[2], dbt.type_float()) ~ ", " ~ quality_ts(now) ~ ")"
            ) %}
          {% endif %}
        {% endif %}
      {% endfor %}
      {% if fresh_rows %}
        {% call statement('quality_insert_freshness') %}
          insert into {{ quality_relation('source_freshness') }}
            (invocation_id, run_started_at, source_table, max_recorded_at, max_sequence, lag_seconds, recorded_at)
          values {{ fresh_rows | join(',\n               ') }}
        {% endcall %}
      {% endif %}
    {% endif %}
    {% do adapter.commit() %}
  {% endif %}
{% endmacro %}
