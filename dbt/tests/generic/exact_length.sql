{#-
    Échoue sur les valeurs dont la longueur diffère de `length`. Les `null` sont
    ignorés : leur présence relève du test `not_null`.
-#}
{% test exact_length(model, column_name, length) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and length({{ column_name }}) <> {{ length }}
{% endtest %}
