{#-
    Une ligne par heure : intensité carbone, couleur Tempo, période tarifaire et prix.

    C'est la table qui répond à « quand consommer ? » pour un particulier :

    * **Heure en UTC comme clé.** Tronquer l'heure locale dupliquerait 2 h–3 h le
      jour du retour à l'heure d'hiver ; l'heure de Paris est un attribut.
    * **Jour Tempo décalé de 6 h.** La couleur du jour J vaut de J 6 h à J+1 6 h :
      les heures creuses de 0 h à 6 h prennent la couleur de la veille.
    * **Heures creuses Tempo fixes**, de 22 h à 6 h, quel que soit le compteur.
    * **Prix nul si inconnu.** Hors grille tarifaire (`tarifs_tempo`) ou sans
      couleur publiée, le prix reste nul : le rapport le signale au lieu de deviner.
-#}
with mesures as (
    select
        date_trunc('hour', date_heure) as heure_utc,
        pas_minutes,
        consommation_mw,
        production_totale_mw,
        taux_co2_g_kwh,
        part_bas_carbone,
        part_renouvelable
    from {{ ref('fct_mix') }}
),

heures as (
    select
        heure_utc,
        count(*) as nb_mesures,
        sum(pas_minutes) as minutes_couvertes,
        avg(consommation_mw) as consommation_mw,
        -- Intensité de l'heure pondérée par la production de chaque mesure.
        sum(taux_co2_g_kwh * production_totale_mw)
            / nullif(sum(production_totale_mw) filter (where taux_co2_g_kwh is not null), 0)
            as taux_co2_g_kwh,
        avg(part_bas_carbone) as part_bas_carbone,
        avg(part_renouvelable) as part_renouvelable
    from mesures
    group by heure_utc
),

calendrier as (
    select
        *,
        timezone('Europe/Paris', heure_utc) as heure_paris,
        cast(timezone('Europe/Paris', heure_utc) as date) as jour,
        hour(timezone('Europe/Paris', heure_utc)) as heure_locale
    from heures
),

tarifaire as (
    select
        *,
        case when heure_locale >= 22 or heure_locale < 6 then 'hc' else 'hp' end
            as periode_tarifaire,
        case when heure_locale < 6 then jour - 1 else jour end as jour_tempo
    from calendrier
)

select
    tarifaire.heure_utc,
    tarifaire.heure_paris,
    tarifaire.jour,
    date_trunc('month', tarifaire.jour)::date as mois,
    tarifaire.heure_locale,
    isodow(tarifaire.jour) as jour_semaine,
    tarifaire.jour_tempo,
    tempo.couleur as couleur_tempo,
    tarifaire.periode_tarifaire,
    tarif.prix_ttc_eur_kwh,
    tarifaire.taux_co2_g_kwh,
    tarifaire.consommation_mw,
    tarifaire.part_bas_carbone,
    tarifaire.part_renouvelable,
    tarifaire.nb_mesures,
    tarifaire.minutes_couvertes >= 60 as est_complete
from tarifaire
left join {{ ref('tempo_jours') }} as tempo
    on tempo.jour_tempo = tarifaire.jour_tempo
left join {{ ref('tarifs_tempo') }} as tarif
    on tarif.couleur = tempo.couleur
    and tarif.periode = tarifaire.periode_tarifaire
    and tarifaire.jour >= tarif.date_debut
    and tarifaire.jour < coalesce(tarif.date_fin, '9999-12-31'::date)
