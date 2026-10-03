{#-
    Aucune mesure ne doit empiéter sur la suivante : chaque ligne couvre
    `[date_heure, date_heure + pas_minutes)`. Un chevauchement signifierait
    qu'un même intervalle est compté deux fois dans les énergies journalières,
    typiquement du temps réel glissé entre deux demi-heures consolidées.
-#}
with mesures as (
    select
        date_heure,
        date_heure + to_minutes(pas_minutes) as fin,
        lead(date_heure) over (order by date_heure) as suivante
    from {{ ref('mix_unifie') }}
)

select *
from mesures
where suivante < fin
