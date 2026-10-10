# `dbt/` — couches silver et gold

Projet **dbt-duckdb** qui construit les couches silver et gold de l'architecture médaillon à
partir des tables `bronze` alimentées par le package Python `ingestion`.

```bash
# depuis la racine du dépôt
uv sync --group dbt
uv run dbt build --project-dir dbt --profiles-dir dbt
uv run dbt docs generate --project-dir dbt --profiles-dir dbt   # catalogue + lignage
```

La base visée est `ECO2MIX_DUCKDB_PATH` (par défaut `data/warehouse/eco2mix.duckdb`, relatif
à la racine du dépôt). Aucun package dbt externe : le projet se construit hors ligne.

## Modèles

```
bronze.national_tr ───────► silver.national_tr ────────┐
                                                       ├──► silver.mix_unifie ──► gold.fct_production_filiere ──► gold.fct_mix ──► gold.fct_mix_journalier
bronze.national_cons_def ─► silver.national_cons_def ──┘                                  ▲
                                                                       gold.dim_filiere (seed)
```

| Modèle | Grain | Rôle |
|---|---|---|
| `silver.national_tr` | 15 min mesuré | Temps réel sans la ligne « à blanc » publiée avant ses mesures |
| `silver.national_cons_def` | 30 min mesuré | Consolidé/définitif sans les quarts d'heure vides (:15, :45), détail par sous-filière |
| `silver.mix_unifie` | mesure | Consolidé/définitif sur sa période, temps réel ensuite ; `qualite` et `pas_minutes` explicites |
| `gold.dim_filiere` | filière | Famille, renouvelable, bas-carbone, ordre et couleur d'affichage |
| `gold.fct_production_filiere` | mesure × filière | Format long pour les graphiques empilés |
| `gold.fct_mix` | mesure | Consommation, prévisions, production par filière et par famille, parts, échanges, CO₂ |
| `gold.fct_mix_journalier` | jour (Paris) | Énergies (GWh), pointe, parts, émissions (tCO₂), `est_complet` |
| `silver.tempo_jours` | jour Tempo | Couleur du jour (bleu, blanc, rouge), valable de 6 h à 6 h le lendemain |
| `gold.tarifs_tempo` | grille × couleur × période | Prix TTC du kWh Tempo (seed, à mettre à jour à chaque mouvement tarifaire) |
| `gold.fct_creneau_horaire` | heure (clé UTC) | Intensité CO₂, couleur Tempo, période hc/hp, prix : base du rapport mensuel |

### Diagnostic territorial

```
bronze.consommation_annuelle_par_iris ──► silver.conso_industrielle_iris ──┬──► gold.fct_pression_industrielle_commune
                                                                           └──► gold.fct_pression_industrielle_departement ──┐
bronze.equilibre_regional_mensuel_… ────► silver.equilibre_regional_mensuel ──┐                                              ├──► gold.fct_diagnostic_territoire
bronze.energies_et_puissances_…_contraintes ► silver.contraintes_reseau_region ┴──► gold.fct_tension_reseau_region ──────────┘
                  seeds : dim_region · dim_departement · seuils_raccordement · limites_methodologiques
```

| Modèle | Grain | Rôle |
|---|---|---|
| `silver.conso_industrielle_iris` | année × IRIS | Code IRIS complété sur 9 caractères, doublons vides écartés, commune reconstituée, hiérarchie résolue par le référentiel |
| `silver.equilibre_regional_mensuel` | mois × région | Production et consommation brute (MWh), `mois` en date |
| `silver.contraintes_reseau_region` | région | Code INSEE retrouvé depuis le libellé publié en majuscules |
| `gold.dim_region`, `gold.dim_departement` | région, département | Référentiel territorial (seeds) ; `est_zni` pour la Corse et l'outre-mer |
| `gold.fct_pression_industrielle_commune` | commune | Consommation et sites raccordés au transport, millésime de référence |
| `gold.fct_pression_industrielle_departement` | département (101) | Idem au département, rang national, y compris les départements sans site |
| `gold.fct_tension_reseau_region` | région (18) | Couverture production / consommation sur 12 mois glissants, rang de dépendance, contraintes d'évacuation |
| `gold.fct_diagnostic_territoire` | département | Tout le diagnostic en une ligne ; la commune s'y joint à la lecture |
| `gold.seuils_raccordement` | tranche de puissance | Domaine de tension et gestionnaire (Enedis / RTE), zone grise 20–40 MW |
| `gold.limites_methodologiques` | indicateur | Source, maille, millésime et limite de chaque indicateur |

## Décisions de modélisation

- **Arbitrage par période, pas par pas de temps.** Le consolidé ne mesure qu'à la demi-heure ;
  combler ses :15 et :45 avec du temps réel compterait deux fois le même quart d'heure. Le
  temps réel ne démarre donc qu'à `dernière mesure consolidée + 30 min`. Le test singulier
  `assert_mix_sans_chevauchement` le garantit.
- **Énergie = puissance × durée.** Les agrégats journaliers somment `MW × pas_minutes / 60`,
  ce qui reste juste avec un pas variable et les jours de 23 ou 25 h.
- **Jours en heure de Paris, session en UTC.** La conversion est explicite
  (`timezone('Europe/Paris', ...)`), le profil fixe `TimeZone: UTC` : le résultat ne dépend
  pas du poste.
- **Reconstruction complète.** Toutes les tables sont matérialisées en `table` et recalculées
  à chaque run : au volume actuel (moins d'un million de lignes), c'est quelques secondes et
  bien plus simple qu'un modèle incrémental.
- **Jour Tempo décalé de 6 h.** Dans `fct_creneau_horaire`, les heures de 0 h à 6 h prennent
  la couleur de la veille ; les heures creuses Tempo sont fixes (22 h–6 h). Sans couleur ou
  hors grille tarifaire, le prix reste nul plutôt que deviné.
- **Jointures par code INSEE, jamais par libellé.** Un seul modèle retrouve un code à partir
  d'un nom (`contraintes_reseau_region`, via la macro `normalise_libelle`), et un test
  `not_null` y interdit toute perte silencieuse. Le test singulier
  `assert_hierarchie_territoriale_conforme` vérifie que le référentiel local redonne les codes
  département et région publiés par ODRÉ, ligne à ligne.
- **Millésime sans secret statistique.** La macro `annee_reference_pression` retient la
  dernière année publiée sans valeur masquée : depuis 2022, ~40 % des IRIS le sont, et un
  classement des départements sur l'année la plus récente serait faux.
- **Regroupements dans un seed.** Les familles bas-carbone, renouvelable et fossile viennent
  de `dim_filiere`, jamais recopiées dans le SQL.

## Tests

Tests génériques (`unique`, `not_null`, `accepted_values`, `relationships`) et maison
(`accepted_range`, `unique_combination`, `exact_length` dans `tests/generic/`). Les bornes physiques
(consommation, taux de CO₂) sont en `severity: warn` : une valeur aberrante publiée par RTE doit
être signalée sans bloquer le pipeline horaire.

Le test Python [`ingestion/tests/test_dbt.py`](../ingestion/tests/test_dbt.py) joue
`dbt build` de bout en bout sur une base temporaire et vérifie les chiffres produits.
