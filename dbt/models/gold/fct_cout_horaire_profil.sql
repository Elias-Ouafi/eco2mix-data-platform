{#-
    Coût et carbone d'un profil de consommation type, heure par heure.

    * **Profil** : puissance appelée par plage horaire, jours ouvrés et
      week-end (seed `profils_consommation`). Une heure dure une heure, donc
      l'énergie de l'heure en kWh vaut la puissance en kW ; un jour de 25 h
      compte bien 25 heures. Les jours fériés sont traités comme des jours ouvrés.
    * **Prix** : option Heures Creuses du tarif bleu non résidentiel 36 kVA
      (seed `tarifs_hphc`), en € HTVA = prix HT + accise. Une entreprise récupère
      la TVA : c'est son coût réel. Hors grille, le prix reste nul.
    * **Heures creuses de 22 h à 6 h**, la plage la plus répandue : en réalité
      fixée localement par le gestionnaire de réseau (voir `limites_methodologiques`).
    * **Carbone** : intensité nationale de l'heure (`fct_creneau_horaire`). Il
      n'existe pas d'intensité régionale opposable, et le prix est identique en
      tout point du territoire (péréquation) : ce coût ne dépend pas de l'adresse.
-#}
with creneaux as (
    select
        heure_utc,
        jour,
        mois,
        heure_locale,
        case when jour_semaine <= 5 then 'ouvre' else 'week_end' end as type_jour,
        periode_tarifaire,
        taux_co2_g_kwh,
        est_complete
    from {{ ref('fct_creneau_horaire') }}
),

tarifs as (
    select
        *,
        prix_ht_eur_kwh + accise_eur_kwh as prix_htva_eur_kwh
    from {{ ref('tarifs_hphc') }}
)

select
    profil.profil,
    creneaux.heure_utc,
    creneaux.jour,
    creneaux.mois,
    creneaux.heure_locale,
    creneaux.type_jour,
    creneaux.periode_tarifaire,
    creneaux.est_complete,
    profil.puissance_kw as conso_kwh,
    profil.puissance_flexible_kw as conso_flexible_kwh,
    tarifs.prix_htva_eur_kwh,
    profil.puissance_kw * tarifs.prix_htva_eur_kwh as cout_htva_eur,
    creneaux.taux_co2_g_kwh,
    profil.puissance_kw * creneaux.taux_co2_g_kwh / 1000 as emissions_kgco2
from creneaux
inner join {{ ref('profils_consommation') }} as profil
    on profil.type_jour = creneaux.type_jour
    and creneaux.heure_locale >= profil.heure_debut
    and creneaux.heure_locale < profil.heure_fin
left join tarifs
    on tarifs.periode = creneaux.periode_tarifaire
    and creneaux.jour >= tarifs.date_debut
    and creneaux.jour < coalesce(tarifs.date_fin, '9999-12-31'::date)
