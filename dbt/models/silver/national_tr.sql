{#-
    Temps réel nettoyé : une ligne par quart d'heure effectivement mesuré.

    RTE publie l'horodatage le plus récent avant ses mesures : une ligne sans
    consommation n'est pas une mesure, elle est écartée ici plutôt que de
    propager des zéros ou des trous dans les agrégats.
-#}
select
    date_heure,
    {{ qualite_depuis_nature('nature') }} as qualite,
    {{ rang_qualite(qualite_depuis_nature('nature')) }} as rang_qualite,
    15 as pas_minutes,
    {% for mesure in mesures_communes() -%}
    {{ mesure }},
    {% endfor -%}
    source_dataset,
    ingested_at_utc
from {{ source('bronze', 'national_tr') }}
where consommation is not null
