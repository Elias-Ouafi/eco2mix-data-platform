{#-
    Code département d'un code commune INSEE : 3 caractères outre-mer (971…976),
    2 ailleurs, Corse comprise (2A, 2B). Pendant SQL de
    `ingestion.diagnostic.code_departement`.
-#}
{% macro code_departement(code_commune) -%}
    case
        when left({{ code_commune }}, 2) = '97' then left({{ code_commune }}, 3)
        else left({{ code_commune }}, 2)
    end
{%- endmacro %}
