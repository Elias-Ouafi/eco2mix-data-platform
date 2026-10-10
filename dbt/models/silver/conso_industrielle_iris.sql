{#-
    Consommation électrique annuelle des sites raccordés au transport, par IRIS.

    Trois réparations, toutes déduites de la source elle-même :

    * **`code_iris` sur 9 caractères.** ODRÉ publie certaines lignes avec un
      code à 8 caractères : le zéro de tête a été perdu en amont (« 20030000 »
      pour l'IRIS 020030000 de l'Aisne). Le code est complété à gauche. En 2022
      et 2023, une vingtaine de ces lignes doublonnent, entièrement vides, une
      ligne correctement codée : la ligne publiée sur 9 caractères est retenue,
      sinon le doublon vide passerait pour du secret statistique.
    * **Commune reconstituée.** Ces mêmes lignes n'ont ni commune ni
      département. Les 5 premiers caractères d'un code IRIS sont le code de la
      commune : on les reprend. Il s'agit souvent d'une commune fusionnée depuis,
      que la BAN ne renverra plus, mais la ligne compte bien dans son département.
    * **Arrondissements ramenés à la commune.** Paris, Lyon et Marseille sont
      publiés sous le code de la commune (75056, 69123, 13055) ; les IRIS
      reconstitués le sont de même.

    Le département est déduit du code commune, et la région du référentiel
    `dim_departement` : un `code_insee_commune` résout toute sa hiérarchie sans
    appel réseau. La consommation est en MWh ; une valeur nulle signale le
    secret statistique (depuis 2022), pas une absence de consommation.
-#}
with source as (
    select
        cast(annee as integer) as annee,
        lpad(code_iris, 9, '0') as code_iris,
        code_insee_commune as code_insee_commune_publie,
        commune,
        consommation_electricite_rte,
        pdl_electricite_rte
    from {{ source('bronze', 'consommation_annuelle_par_iris') }}
    qualify row_number() over (
        partition by annee, lpad(code_iris, 9, '0')
        order by length(code_iris) desc
    ) = 1
),

communes as (
    select
        *,
        coalesce(
            code_insee_commune_publie,
            case
                when left(code_iris, 3) = '751' then '75056'
                when left(code_iris, 3) = '693' then '69123'
                when left(code_iris, 3) = '132' then '13055'
                else left(code_iris, 5)
            end
        ) as code_insee_commune
    from source
)

select
    communes.annee,
    communes.code_iris,
    communes.code_insee_commune,
    communes.commune,
    communes.code_insee_commune_publie is null as est_commune_reconstituee,
    {{ code_departement('communes.code_insee_commune') }} as code_departement,
    departement.code_insee_region,
    communes.consommation_electricite_rte as consommation_electricite_mwh,
    communes.pdl_electricite_rte as nb_sites_electricite,
    communes.consommation_electricite_rte is null as est_secret
from communes
left join {{ ref('dim_departement') }} as departement
    on departement.code_departement = {{ code_departement('communes.code_insee_commune') }}
