{#-
    Production par filière au format long : une ligne par mesure et par filière.
    Format attendu par les graphiques empilés et par les agrégats par famille.
-#}
with production as (
    unpivot (
        select date_heure, qualite, pas_minutes, {{ filieres() | join(', ') }}
        from {{ ref('mix_unifie') }}
    )
    on {{ filieres() | join(', ') }}
    into name filiere value production_mw
)

select
    production.date_heure,
    timezone('Europe/Paris', production.date_heure) as date_heure_paris,
    cast(timezone('Europe/Paris', production.date_heure) as date) as jour,
    production.qualite,
    production.pas_minutes,
    production.filiere,
    filiere.libelle,
    filiere.famille,
    filiere.est_renouvelable,
    filiere.est_bas_carbone,
    production.production_mw,
    production.production_mw * production.pas_minutes / 60 as energie_mwh
from production
inner join {{ ref('dim_filiere') }} as filiere on filiere.filiere = production.filiere
