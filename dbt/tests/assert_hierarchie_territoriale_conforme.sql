{#-
    La hiérarchie commune → département → région déduite localement (macro
    `code_departement` + seed `dim_departement`) doit redonner exactement les
    codes publiés par ODRÉ, partout où ODRÉ les publie. Un écart signalerait une
    erreur du référentiel, qui fausserait le diagnostic sans bruit.
-#}
select
    bronze.annee,
    bronze.code_iris,
    bronze.code_insee_departement as departement_publie,
    silver.code_departement as departement_deduit,
    bronze.code_insee_region as region_publiee,
    silver.code_insee_region as region_deduite
from {{ source('bronze', 'consommation_annuelle_par_iris') }} as bronze
inner join {{ ref('conso_industrielle_iris') }} as silver
    on silver.annee = cast(bronze.annee as integer)
    and silver.code_iris = lpad(bronze.code_iris, 9, '0')
where bronze.code_insee_departement is not null
  and (
    bronze.code_insee_departement <> silver.code_departement
    or bronze.code_insee_region <> silver.code_insee_region
  )
