{#-
    Série unique du mix électrique, à la meilleure qualité disponible.

    Le consolidé/définitif fait foi sur toute la période qu'il couvre ; le temps
    réel ne prend le relais qu'après sa dernière mesure. L'arbitrage se fait par
    période et non pas de temps par pas de temps : le consolidé ne mesure qu'à la
    demi-heure, et compléter ses :15 et :45 par du temps réel compterait deux
    fois le même quart d'heure dans les sommes d'énergie.

    Le temps réel démarre donc à `fin du consolidé + 30 min`, c'est-à-dire à la
    fin de l'intervalle couvert par la dernière demi-heure consolidée.
-#}
with borne as (
    select max(date_heure) + interval 30 minute as debut_temps_reel
    from {{ ref('national_cons_def') }}
),

consolide as (
    select
        date_heure, qualite, rang_qualite, pas_minutes,
        {% for mesure in mesures_communes() -%}
        {{ mesure }},
        {% endfor -%}
        source_dataset,
        ingested_at_utc
    from {{ ref('national_cons_def') }}
),

temps_reel as (
    select
        date_heure, qualite, rang_qualite, pas_minutes,
        {% for mesure in mesures_communes() -%}
        {{ mesure }},
        {% endfor -%}
        source_dataset,
        ingested_at_utc
    from {{ ref('national_tr') }}
    where date_heure >= coalesce((select debut_temps_reel from borne), '-infinity'::timestamptz)
)

select * from consolide
union all
select * from temps_reel
