{#-
    Bilan mensuel d'un profil type : consommation, coût, carbone, et gain d'un
    décalage de la part flexible vers les heures creuses.

    **Gain de décalage.** Chaque jour, la consommation flexible tombée en heures
    pleines est reportée en heures creuses du même jour civil. Le gain en euros
    est l'écart de prix HP/HC ; le CO₂ évité compare l'intensité des heures où la
    consommation avait lieu à l'intensité **moyenne** des heures creuses du jour
    — pas à la meilleure heure, pour ne pas promettre plus que ce qu'un
    programmateur simple obtiendrait.

    **Coût nul** si une heure du mois est hors grille tarifaire : un total
    partiel serait pris pour un total. `est_complet` exige que toutes les heures
    du mois (heure de Paris, 23 ou 25 h les jours de changement d'heure) soient
    mesurées.
-#}
with horaire as (
    select * from {{ ref('fct_cout_horaire_profil') }}
),

jours as (
    select
        profil,
        mois,
        jour,
        sum(conso_flexible_kwh) filter (where periode_tarifaire = 'hp') as kwh_decales,
        sum(conso_flexible_kwh * prix_htva_eur_kwh) filter (where periode_tarifaire = 'hp')
            as cout_flexible_hp_eur,
        sum(conso_flexible_kwh * taux_co2_g_kwh) filter (where periode_tarifaire = 'hp') / 1000
            as emissions_flexible_hp_kgco2,
        max(prix_htva_eur_kwh) filter (where periode_tarifaire = 'hc') as prix_hc_eur_kwh,
        avg(taux_co2_g_kwh) filter (where periode_tarifaire = 'hc') as taux_co2_hc_g_kwh
    from horaire
    group by profil, mois, jour
),

gains as (
    select
        profil,
        mois,
        sum(kwh_decales) as kwh_decales,
        sum(cout_flexible_hp_eur - kwh_decales * prix_hc_eur_kwh) as gain_decalage_eur,
        sum(emissions_flexible_hp_kgco2 - kwh_decales * taux_co2_hc_g_kwh / 1000)
            as co2_evite_kg
    from jours
    group by profil, mois
),

mois as (
    select
        profil,
        mois,
        count(*) as nb_heures,
        bool_and(est_complete) as heures_completes,
        sum(conso_kwh) as conso_kwh,
        sum(conso_kwh) filter (where periode_tarifaire = 'hc') as conso_hc_kwh,
        count(prix_htva_eur_kwh) = count(*) as est_tarife,
        sum(cout_htva_eur) as cout_htva_eur,
        sum(emissions_kgco2) as emissions_kgco2
    from horaire
    group by profil, mois
)

select
    mois.profil,
    mois.mois,
    mois.nb_heures,
    datediff(
        'hour',
        timezone('Europe/Paris', mois.mois::timestamp),
        timezone('Europe/Paris', (mois.mois + interval 1 month)::timestamp)
    ) as nb_heures_attendues,
    mois.heures_completes
    and mois.nb_heures = datediff(
        'hour',
        timezone('Europe/Paris', mois.mois::timestamp),
        timezone('Europe/Paris', (mois.mois + interval 1 month)::timestamp)
    ) as est_complet,
    mois.conso_kwh,
    mois.conso_hc_kwh / mois.conso_kwh as part_hc,
    case when mois.est_tarife then mois.cout_htva_eur end as cout_htva_eur,
    case when mois.est_tarife then mois.cout_htva_eur / mois.conso_kwh end as prix_moyen_htva_eur_kwh,
    mois.emissions_kgco2,
    mois.emissions_kgco2 * 1000 / mois.conso_kwh as intensite_moyenne_g_kwh,
    gains.kwh_decales,
    case when mois.est_tarife then gains.gain_decalage_eur end as gain_decalage_eur,
    gains.co2_evite_kg
from mois
inner join gains on gains.profil = mois.profil and gains.mois = mois.mois
