{#- Unicité d'une combinaison de colonnes, sans dépendre du package dbt_utils. -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
