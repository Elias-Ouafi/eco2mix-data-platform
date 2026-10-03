{#-
    Mix électrique au pas de mesure (30 min en consolidé, 15 min en temps réel).

    Les regroupements bas-carbone, renouvelable et fossile viennent du seed
    `dim_filiere`, via la table longue : aucune liste de familles n'est
    recopiée ici.
-#}
with familles as (
    select
        date_heure,
        sum(production_mw) as production_totale_mw,
        sum(production_mw) filter (where est_bas_carbone) as production_bas_carbone_mw,
        sum(production_mw) filter (where est_renouvelable) as production_renouvelable_mw,
        sum(production_mw) filter (where famille = 'fossile') as production_fossile_mw
    from {{ ref('fct_production_filiere') }}
    group by date_heure
)

select
    mix.date_heure,
    timezone('Europe/Paris', mix.date_heure) as date_heure_paris,
    cast(timezone('Europe/Paris', mix.date_heure) as date) as jour,
    mix.qualite,
    mix.rang_qualite,
    mix.pas_minutes,

    mix.consommation as consommation_mw,
    mix.prevision_j1 as prevision_j1_mw,
    mix.prevision_j as prevision_j_mw,
    mix.consommation - mix.prevision_j1 as ecart_prevision_j1_mw,

    {% for filiere in filieres() -%}
    mix.{{ filiere }} as {{ filiere }}_mw,
    {% endfor -%}
    mix.pompage as pompage_mw,

    coalesce(familles.production_totale_mw, 0) as production_totale_mw,
    coalesce(familles.production_bas_carbone_mw, 0) as production_bas_carbone_mw,
    coalesce(familles.production_renouvelable_mw, 0) as production_renouvelable_mw,
    coalesce(familles.production_fossile_mw, 0) as production_fossile_mw,
    familles.production_bas_carbone_mw / nullif(familles.production_totale_mw, 0)
        as part_bas_carbone,
    familles.production_renouvelable_mw / nullif(familles.production_totale_mw, 0)
        as part_renouvelable,

    -- Convention RTE : un solde négatif est un export net.
    mix.ech_physiques as solde_echanges_mw,
    mix.taux_co2 as taux_co2_g_kwh
from {{ ref('mix_unifie') }} as mix
left join familles on familles.date_heure = mix.date_heure
