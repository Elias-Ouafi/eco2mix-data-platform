{#-
    Tension du réseau régional : une ligne par région du référentiel.

    * **Équilibre sur 12 mois glissants**, les 12 derniers mois publiés pour
      l'ensemble des régions. `taux_couverture` = production / consommation
      brute : sous 1, la région importe de ses voisines.
    * **Contraintes d'évacuation** (instantané RTE) : puissance à compenser et
      énergie renouvelable non évacuée, sommée sur les quatre saisons.

    `rang_dependance` classe les régions de la plus déficitaire (1) à la plus
    excédentaire. Les régions sans donnée (outre-mer) restent présentes, à nul :
    le diagnostic doit pouvoir dire « pas de donnée » plutôt que de perdre la ligne.
-#}
with mensuel as (
    select * from {{ ref('equilibre_regional_mensuel') }}
),

fenetre as (
    select
        cast(max(mois) - interval 11 month as date) as mois_debut,
        max(mois) as mois_fin
    from mensuel
),

equilibre as (
    select
        mensuel.code_insee_region,
        count(*) as nb_mois,
        sum(mensuel.production_totale_mwh) as production_mwh,
        sum(mensuel.consommation_brute_mwh) as consommation_mwh
    from mensuel
    inner join fenetre on mensuel.mois between fenetre.mois_debut and fenetre.mois_fin
    group by mensuel.code_insee_region
),

regions as (
    select
        region.code_insee_region,
        region.region,
        region.est_zni,
        fenetre.mois_debut,
        fenetre.mois_fin,
        coalesce(equilibre.nb_mois, 0)::integer as nb_mois,
        equilibre.production_mwh / 1000 as production_gwh,
        equilibre.consommation_mwh / 1000 as consommation_gwh,
        (equilibre.production_mwh - equilibre.consommation_mwh) / 1000 as solde_gwh,
        equilibre.production_mwh / nullif(equilibre.consommation_mwh, 0) as taux_couverture,
        contraintes.puissance_enr_installee_mw,
        contraintes.puissance_a_compenser_mw,
        contraintes.energie_non_evacuee_printemps_mwh
            + contraintes.energie_non_evacuee_ete_mwh
            + contraintes.energie_non_evacuee_automne_mwh
            + contraintes.energie_non_evacuee_hiver_mwh as energie_non_evacuee_mwh
    from {{ ref('dim_region') }} as region
    cross join fenetre
    left join equilibre on equilibre.code_insee_region = region.code_insee_region
    left join {{ ref('contraintes_reseau_region') }} as contraintes
        on contraintes.code_insee_region = region.code_insee_region
)

select
    *,
    case
        when taux_couverture is null then null
        when taux_couverture >= 1 then 'excedentaire'
        else 'deficitaire'
    end as statut_equilibre,
    case
        when taux_couverture is not null
            then rank() over (partition by taux_couverture is null order by taux_couverture)
    end as rang_dependance,
    count(taux_couverture) over () as nb_regions_classees
from regions
