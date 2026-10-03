{#-
    Bilan journalier, jour civil en heure de Paris.

    Les énergies sont des sommes `puissance × durée` et non des moyennes × 24 :
    un jour de changement d'heure dure 23 ou 25 h, et le pas varie entre le
    consolidé (30 min) et le temps réel (15 min). `est_complet` signale les jours
    partiels — typiquement le jour en cours — dont les totaux sont tronqués.
-#}
with mesures as (
    select
        *,
        pas_minutes / 60.0 as heures
    from {{ ref('fct_mix') }}
),

jours as (
    select
        jour,
        -- La qualité d'une journée est celle de sa mesure la moins fiable.
        arg_min(qualite, rang_qualite) as qualite,
        count(*) as nb_mesures,
        sum(heures) as heures_couvertes,

        sum(consommation_mw * heures) / 1000 as consommation_gwh,
        max(consommation_mw) as pointe_consommation_mw,
        min(consommation_mw) as creux_consommation_mw,

        sum(production_totale_mw * heures) / 1000 as production_gwh,
        {% for filiere in filieres() -%}
        sum(coalesce({{ filiere }}_mw, 0) * heures) / 1000 as {{ filiere }}_gwh,
        {% endfor -%}
        sum(production_bas_carbone_mw * heures)
            / nullif(sum(production_totale_mw * heures), 0) as part_bas_carbone,
        sum(production_renouvelable_mw * heures)
            / nullif(sum(production_totale_mw * heures), 0) as part_renouvelable,

        sum(solde_echanges_mw * heures) / 1000 as solde_echanges_gwh,

        -- gCO2/kWh x MWh = kgCO2 ; / 1000 pour des tonnes.
        sum(taux_co2_g_kwh * production_totale_mw * heures) / 1000 as emissions_tco2,
        sum(taux_co2_g_kwh * production_totale_mw * heures) / nullif(
            sum(production_totale_mw * heures) filter (where taux_co2_g_kwh is not null), 0
        ) as taux_co2_moyen_g_kwh
    from mesures
    group by jour
),

durees as (
    select
        *,
        date_diff(
            'minute',
            timezone('Europe/Paris', jour::timestamp),
            timezone('Europe/Paris', (jour + 1)::timestamp)
        ) / 60.0 as heures_du_jour
    from jours
)

select
    jour,
    qualite,
    nb_mesures,
    heures_couvertes,
    heures_du_jour,
    heures_couvertes >= heures_du_jour as est_complet,
    * exclude (jour, qualite, nb_mesures, heures_couvertes, heures_du_jour)
from durees
