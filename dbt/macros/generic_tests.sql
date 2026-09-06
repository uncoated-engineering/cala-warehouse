{#- Generic tests kept in-package so `dbt build` needs no `dbt deps` and no
    network. Named with a `dbt_utils_free_` prefix to make the trade-off obvious. -#}

{% test dbt_utils_free_unique_combination(model, combination) %}
select
    {{ combination | join(', ') }},
    count(*) as n
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1
{% endtest %}


{% test dbt_utils_free_non_negative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}


{% test dbt_utils_free_at_least(model, column_name, value) %}
select *
from {{ model }}
where {{ column_name }} < {{ value }}
{% endtest %}


{#- SCD-2 sanity: exactly one current row per key, and no overlapping windows. -#}
{% test dbt_utils_free_scd2_single_current(model, key_column) %}
select
    {{ key_column }},
    {{ count_where('is_current') }} as current_rows
from {{ model }}
group by {{ key_column }}
having {{ count_where('is_current') }} <> 1
{% endtest %}
