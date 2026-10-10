{#-
    Diagnostic de contexte d'implantation : une ligne par département.

    **Pourquoi le département, et pas la commune.** Aucun jeu ouvert du projet ne
    liste les ~35 000 communes, et seules ~1 300 ont un site raccordé au
    transport : une table à la commune serait vide presque partout. Le
    département est la plus fine maille renseignée partout ; la commune vient en
    complément, par jointure sur `fct_pression_industrielle_commune` quand elle y
    figure (voir `ingestion/diagnostic.py`, qui fait les deux en un `SELECT`).

    Chaque indicateur a sa limite dans `limites_methodologiques`.
-#}
select
    pression.code_departement,
    pression.departement,
    tension.code_insee_region,
    tension.region,
    tension.est_zni,

    -- Pression industrielle du département (dernière année sans secret statistique)
    pression.annee as annee_pression,
    pression.consommation_electricite_mwh / 1000 as conso_industrielle_departement_gwh,
    pression.nb_sites_electricite as nb_sites_departement,
    pression.nb_communes_avec_site,
    pression.est_minorant as conso_departement_est_minorant,
    pression.rang_national as rang_pression_departement,
    pression.nb_departements_classes,

    -- Tension du réseau régional (12 mois glissants + contraintes RTE)
    tension.mois_debut as equilibre_mois_debut,
    tension.mois_fin as equilibre_mois_fin,
    tension.nb_mois as equilibre_nb_mois,
    tension.production_gwh as production_region_gwh,
    tension.consommation_gwh as consommation_region_gwh,
    tension.solde_gwh as solde_region_gwh,
    tension.taux_couverture as taux_couverture_region,
    tension.statut_equilibre,
    tension.rang_dependance as rang_dependance_region,
    tension.nb_regions_classees,
    tension.puissance_enr_installee_mw,
    tension.puissance_a_compenser_mw,
    tension.energie_non_evacuee_mwh
from {{ ref('fct_pression_industrielle_departement') }} as pression
inner join {{ ref('fct_tension_reseau_region') }} as tension
    on tension.code_insee_region = pression.code_insee_region
