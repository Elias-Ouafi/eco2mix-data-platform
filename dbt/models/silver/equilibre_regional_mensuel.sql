{#-
    Équilibre mensuel production / consommation brute par région, en MWh.

    `mois` devient une date (premier jour du mois). Le libellé de région est
    abandonné : la région est identifiée par son code INSEE, et son nom vient du
    référentiel `dim_region`.
-#}
select
    cast(strptime(mois || '-01', '%Y-%m-%d') as date) as mois,
    code_insee_region,
    production_totale as production_totale_mwh,
    consommation_brute as consommation_brute_mwh,
    pompage as pompage_mwh,
    solde_echanges_physiques as solde_echanges_physiques_mwh
from {{ source('bronze', 'equilibre_regional_mensuel_prod_conso_brute') }}
