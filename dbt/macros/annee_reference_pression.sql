{#-
    Millésime de référence de la pression industrielle : la dernière année
    publiée **sans secret statistique**, à défaut la dernière année publiée.

    Depuis 2022, ODRÉ masque ~40 % des IRIS. Sur l'année la plus récente, le Nord
    tomberait de 8,7 à 2,3 TWh et la Savoie de 3,8 à 0,9 TWh : un classement des
    territoires bâti dessus serait faux. Mieux vaut un chiffre plus ancien mais
    complet, millésime affiché.
-#}
{% macro annee_reference_pression(iris_relation) -%}
    select coalesce(
        max(annee) filter (where not a_du_secret),
        max(annee)
    ) as annee
    from (
        select annee, bool_or(est_secret) as a_du_secret
        from {{ iris_relation }}
        group by annee
    )
{%- endmacro %}
