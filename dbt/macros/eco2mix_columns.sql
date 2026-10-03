{#- Listes de colonnes partagées par les modèles, pour ne les écrire qu'une fois. -#}

{#- Filières de production, alignées sur le seed `dim_filiere`. -#}
{% macro filieres() -%}
    {{ return(['nucleaire', 'eolien', 'solaire', 'hydraulique', 'gaz', 'fioul', 'charbon', 'bioenergies']) }}
{%- endmacro %}

{#- Mesures publiées à la fois par le temps réel et par le consolidé/définitif. -#}
{% macro mesures_communes() -%}
    {{ return(
        ['consommation', 'prevision_j1', 'prevision_j']
        + filieres()
        + ['pompage', 'ech_physiques', 'ech_comm_angleterre', 'ech_comm_espagne',
           'ech_comm_italie', 'ech_comm_suisse', 'ech_comm_allemagne_belgique', 'taux_co2']
    ) }}
{%- endmacro %}

{#- Somme de colonnes en traitant `null` comme 0 (filière non renseignée). -#}
{% macro somme(colonnes) -%}
    {%- for colonne in colonnes -%}
        coalesce({{ colonne }}, 0){{ ' + ' if not loop.last }}
    {%- endfor -%}
{%- endmacro %}

{#- Qualité normalisée à partir du libellé `nature` publié par RTE. -#}
{% macro qualite_depuis_nature(colonne) -%}
    case
        when strip_accents(lower({{ colonne }})) like '%definitive%' then 'definitive'
        when strip_accents(lower({{ colonne }})) like '%consolidee%' then 'consolidee'
        when strip_accents(lower({{ colonne }})) like '%temps reel%' then 'temps_reel'
    end
{%- endmacro %}

{#- Rang de qualité : plus il est élevé, plus la donnée fait foi. -#}
{% macro rang_qualite(qualite) -%}
    case {{ qualite }} when 'definitive' then 3 when 'consolidee' then 2 when 'temps_reel' then 1 end
{%- endmacro %}
