{# generic test: composite-key uniqueness without dbt_utils #}
{% test dbt_unique_combo(model, combo) %}
select {{ combo | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combo | join(', ') }}
having count(*) > 1
{% endtest %}
