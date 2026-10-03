{#-
    Calendrier Tempo : une ligne par jour Tempo, couleur normalisée en français.

    Un jour Tempo court de 6 h à 6 h le lendemain : la couleur du jour J
    s'applique aussi aux heures creuses de 0 h à 6 h de J+1. Ce décalage est
    appliqué dans `gold.fct_creneau_horaire`, pas ici : silver reste le miroir
    nettoyé du calendrier publié.
-#}
select
    cast(timezone('Europe/Paris', date_heure) as date) as jour_tempo,
    case couleur
        when 'BLUE' then 'bleu'
        when 'WHITE' then 'blanc'
        when 'RED' then 'rouge'
    end as couleur,
    ingested_at_utc
from {{ source('bronze', 'rte_tempo') }}
where couleur is not null
