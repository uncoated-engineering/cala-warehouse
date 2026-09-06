{#- Use the configured +schema literally (raw / staging / marts) instead of
    dbt's default "<target_schema>_<custom>" concatenation, so sources.yml can
    point at the seed schema by its plain name on every target. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
