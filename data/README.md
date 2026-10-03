# `data/` — stockage local (non versionné)

Ce dossier est **entièrement gitignoré** (sauf ce README). Il est recréé à la demande par le
pipeline : les répertoires manquants sont créés au premier run.

## Structure

```
data/
├── bronze/
│   ├── eco2mix_national_tr/
│   │   └── ingest_date=YYYY-MM-DD/        # partition = date d'ingestion (UTC)
│   │       └── part-<run_id>.parquet      # un fichier par exécution
│   └── eco2mix_national_cons_def/
│       └── ingest_date=YYYY-MM-DD/
│           └── part-<run_id>.parquet
└── warehouse/
    └── eco2mix.duckdb                     # schémas bronze, silver, gold
```

## Les trois couches

| Couche | Emplacement | Écrite par | Mode |
|---|---|---|---|
| Bronze | `bronze/*.parquet` | `ingestion` (Python) | ajout seul, fichiers immuables |
| Bronze | `eco2mix.duckdb` → schéma `bronze` | `ingestion` (Python) | MERGE sur `date_heure` |
| Silver | `eco2mix.duckdb` → schéma `silver` | dbt | reconstruite à chaque run |
| Gold | `eco2mix.duckdb` → schéma `gold` | dbt | reconstruite à chaque run |

### `bronze/` — zone d'atterrissage immuable

Un fichier Parquet par exécution d'un DAG, jamais modifié après écriture. Les partitions
suivent la convention Hive (`ingest_date=...`) pour rester lisibles par DuckDB
(`read_parquet(..., hive_partitioning=true)`) et, plus tard, par BigQuery via une table externe
sur GCS.

La date de partition est la date **d'ingestion**, pas celle de la donnée : une même heure de
production apparaît dans plusieurs partitions, puisque RTE révise ses valeurs. Ces fichiers
sont donc **l'historique complet des révisions**, alors que les tables `bronze` de DuckDB n'en
gardent que la dernière version.

Colonnes techniques ajoutées à l'extraction :

| Colonne | Type | Rôle |
|---|---|---|
| `date_heure` | `timestamp[us, tz=UTC]` | Clé métier, normalisée en UTC |
| `ingested_at_utc` | `timestamp[us, tz=UTC]` | Horodatage du run, arbitre les révisions |
| `source_dataset` | `string` | Dataset ODRÉ d'origine |

### `warehouse/` — DuckDB

Un seul fichier, `eco2mix.duckdb`, qui porte les trois couches sous forme de schémas.
**DuckDB n'accepte qu'un seul writer** : les tâches Airflow qui y touchent (chargement bronze,
`dbt build`, contrôles) passent par le pool `duckdb_writer` (1 slot), et les DAGs tournent en
`max_active_runs=1`. Ne gardez pas de session `duckdb` interactive ouverte pendant un run, elle
prendrait le verrou.

Le chargement bronze est idempotent : chaque run remplace les lignes de même `date_heure`
(`DuckDBLoader.merge_parquet`), donc rejouer une fenêtre ne crée aucun doublon. Silver et gold
sont entièrement reconstruites par dbt à partir de bronze : elles ne contiennent rien qui ne
puisse être recalculé.

## Repartir de zéro

```bash
rm -rf data/bronze data/warehouse
```

Aucune donnée n'est perdue définitivement : tout est ré-extractible depuis l'API ODRÉ (voir la
section *Backfill* du README racine).

Si vous aviez lancé une version antérieure du pipeline, les dossiers `data/raw/` et le schéma
`raw` de DuckDB ne sont plus utilisés : supprimez-les, ou déplacez `data/raw/` en
`data/bronze/` puis relancez un chargement.

## Volumétrie indicative

Au pas 15 minutes, le dataset temps réel représente environ 35 000 lignes par an ; le consolidé
(depuis 2012) environ 500 000 lignes au total. L'ensemble tient en quelques dizaines de
mégaoctets : la reconstruction complète de silver et gold prend quelques secondes.
