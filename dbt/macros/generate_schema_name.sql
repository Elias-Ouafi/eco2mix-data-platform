{#-
    Les schémas portent le nom de la couche (`silver`, `gold`) et non
    `<cible>_<couche>` comme le veut le comportement par défaut de dbt.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
