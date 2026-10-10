{#-
    Contraintes d'évacuation du réseau de transport par région (vision RTE).

    La source identifie la région par son **libellé en majuscules**
    (« AUVERGNE-RHÔNE-ALPES ») : c'est le seul endroit du projet où le code INSEE
    est retrouvé à partir d'un nom, via `normalise_libelle`. Le test
    `not_null` sur `code_insee_region` garantit qu'aucune région n'est perdue
    en silence si RTE change d'écriture.

    Unités publiées par RTE : puissances en MW, énergies non évacuées en MWh par
    saison. L'énergie non évacuée est de la production renouvelable écrêtée,
    faute de capacité d'évacuation : un signal côté injection.
-#}
select
    region.code_insee_region,
    source.region as libelle_publie,
    source.puissance_enr_installee as puissance_enr_installee_mw,
    source.puissance_totale_a_compenser as puissance_a_compenser_mw,
    source.energie_non_evacuee_moyenne_printemps as energie_non_evacuee_printemps_mwh,
    source.energie_non_evacuee_moyenne_ete as energie_non_evacuee_ete_mwh,
    source.energie_non_evacuee_moyenne_automne as energie_non_evacuee_automne_mwh,
    source.energie_non_evacuee_moyenne_hiver as energie_non_evacuee_hiver_mwh
from {{ source('bronze', 'energies_et_puissances_regionales_liees_au_contraintes') }} as source
left join {{ ref('dim_region') }} as region
    on {{ normalise_libelle('region.region') }} = {{ normalise_libelle('source.region') }}
