# `data/` — stockage local (non versionné)

Ce dossier est **entièrement gitignoré** (sauf ce README). Il est recréé à la demande par le
pipeline : les répertoires manquants sont créés au premier run.

## Structure

```
data/
├── raw/
│   └── eco2mix_national_tr/
│       └── ingest_date=YYYY-MM-DD/        # partition = date d'ingestion (UTC)
│           └── part-<run_id>.parquet      # un fichier par exécution
└── warehouse/
    └── eco2mix.duckdb                     # schéma `raw`, table `national_tr`
```

### `raw/` — zone d'atterrissage immuable

Un fichier Parquet par exécution du DAG, jamais modifié après écriture. Les partitions suivent
la convention Hive (`ingest_date=...`) pour rester lisibles par DuckDB
(`read_parquet(..., hive_partitioning=true)`) et, plus tard, par BigQuery via une table externe
sur GCS.

La date de partition est la date **d'ingestion**, pas celle de la donnée : une même heure de
production apparaît dans plusieurs partitions, puisque RTE révise ses valeurs temps réel.

Colonnes techniques ajoutées à l'extraction :

| Colonne | Type | Rôle |
|---|---|---|
| `date_heure` | `timestamp[us, tz=UTC]` | Clé métier, normalisée en UTC |
| `ingested_at_utc` | `timestamp[us, tz=UTC]` | Horodatage du run, arbitre les révisions |
| `source_dataset` | `string` | Dataset ODRÉ d'origine |

### `warehouse/` — DuckDB

Un seul fichier, `eco2mix.duckdb`. **DuckDB n'accepte qu'un seul writer** : les tâches Airflow
qui écrivent passent par le pool `duckdb_writer` (1 slot) et le DAG tourne en
`max_active_runs=1`. Ne gardez pas de session `duckdb` interactive ouverte pendant un run, elle
prendrait le verrou.

Le chargement est idempotent : chaque run remplace les lignes de même `date_heure`
(`DuckDBLoader.merge_parquet`), donc rejouer une fenêtre ne crée aucun doublon.

## Repartir de zéro

```bash
rm -rf data/raw data/warehouse
```

Aucune donnée n'est perdue définitivement : tout est ré-extractible depuis l'API ODRÉ (voir la
section *Backfill* du README racine).

## Volumétrie indicative

Au pas 15 minutes, le dataset national représente environ 35 000 lignes par an, soit quelques
mégaoctets en Parquet compressé : l'ensemble tient largement en local.
