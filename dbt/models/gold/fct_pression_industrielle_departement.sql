{#-
    Pression industrielle par département, pour le millésime de référence
    (dernière année publiée sans secret statistique, voir `annee_reference_pression`).

    Une ligne pour **chacun** des 101 départements du référentiel, y compris ceux
    sans aucun site raccordé au transport (consommation à 0) : l'absence est une
    information, pas une donnée manquante.

    `rang_national` classe les départements par consommation décroissante
    (1 = la plus forte), parmi ceux qui consomment. Les IRIS dont la commune a été
    reconstituée (fusion de communes) comptent ici, même si la commune n'est plus
    géocodable.
-#}
with iris as (
    select * from {{ ref('conso_industrielle_iris') }}
),

reference as (
    {{ annee_reference_pression(ref('conso_industrielle_iris')) }}
),

departements as (
    select
        iris.code_departement,
        sum(iris.consommation_electricite_mwh) as consommation_electricite_mwh,
        sum(iris.nb_sites_electricite) as nb_sites_electricite,
        count(distinct iris.code_insee_commune)
            filter (where iris.nb_sites_electricite > 0) as nb_communes_avec_site,
        count(*) filter (where iris.est_secret) as nb_iris_secret
    from iris
    inner join reference on reference.annee = iris.annee
    group by iris.code_departement
),

complet as (
    select
        reference.annee,
        dim.code_departement,
        dim.departement,
        dim.code_insee_region,
        coalesce(departements.consommation_electricite_mwh, 0) as consommation_electricite_mwh,
        coalesce(departements.nb_sites_electricite, 0)::integer as nb_sites_electricite,
        coalesce(departements.nb_communes_avec_site, 0)::integer as nb_communes_avec_site,
        coalesce(departements.nb_iris_secret, 0)::integer as nb_iris_secret
    from {{ ref('dim_departement') }} as dim
    cross join reference
    left join departements on departements.code_departement = dim.code_departement
)

select
    *,
    nb_iris_secret > 0 as est_minorant,
    case
        when consommation_electricite_mwh > 0
            then rank() over (
                partition by consommation_electricite_mwh > 0
                order by consommation_electricite_mwh desc
            )
    end as rang_national,
    count(*) filter (where consommation_electricite_mwh > 0) over () as nb_departements_classes
from complet
