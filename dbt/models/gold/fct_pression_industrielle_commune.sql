{#-
    Pression industrielle locale : une ligne par commune ayant au moins un IRIS
    publié pour le millésime de référence (macro `annee_reference_pression` :
    dernière année sans secret statistique).

    Seules ~1 300 communes y figurent : l'immense majorité des communes n'a aucun
    site raccordé au réseau de transport, et c'est une information en soi. Le
    diagnostic se replie alors sur le département (`fct_pression_industrielle_departement`).

    `est_minorant` signale qu'au moins un IRIS de la commune est sous secret
    statistique : la consommation affichée est une borne basse.
-#}
with iris as (
    select * from {{ ref('conso_industrielle_iris') }}
),

reference as (
    {{ annee_reference_pression(ref('conso_industrielle_iris')) }}
)

select
    iris.annee,
    iris.code_insee_commune,
    min(iris.commune) as commune,
    min(iris.code_departement) as code_departement,
    min(iris.code_insee_region) as code_insee_region,
    coalesce(sum(iris.consommation_electricite_mwh), 0) as consommation_electricite_mwh,
    coalesce(sum(iris.nb_sites_electricite), 0)::integer as nb_sites_electricite,
    count(*)::integer as nb_iris,
    count(*) filter (where iris.est_secret)::integer as nb_iris_secret,
    bool_or(iris.est_secret) as est_minorant
from iris
inner join reference on reference.annee = iris.annee
group by iris.annee, iris.code_insee_commune
