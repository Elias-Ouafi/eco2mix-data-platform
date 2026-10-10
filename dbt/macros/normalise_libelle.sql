{#-
    Normalise un libellé pour comparaison : sans accent, sans casse, sans
    ponctuation. Pendant SQL de `ingestion.geocode.normalise` en Python.

    « PROVENCE-ALPES-CÔTE D'AZUR » et « Provence-Alpes-Côte d'Azur » donnent
    tous deux `provencealpescotedazur`. Ne sert qu'à retrouver un code INSEE :
    aucune jointure entre tables ne doit se faire sur un libellé.
-#}
{% macro normalise_libelle(column) -%}
    regexp_replace(lower(strip_accents({{ column }})), '[^a-z0-9]', '', 'g')
{%- endmacro %}
