"""Ingestion horaire du mix électrique français (éCO2mix national temps réel).

`extract` -> `load_bronze` -> `dbt_build` -> `freshness_check`

Architecture médaillon : l'ingestion Python alimente la couche **bronze**
(Parquet immuables + `bronze.national_tr`), dbt reconstruit ensuite **silver**
(nettoyage, unification temps réel / consolidé) et **gold** (tables métier).

* **Fenêtre glissante.** Chaque run relit les 3 dernières heures et non la seule
  heure écoulée : RTE révise ses valeurs temps réel après publication, et le
  MERGE sur `date_heure` rend ce recouvrement gratuit.
* **Un seul writer.** DuckDB n'accepte qu'une connexion en écriture. Le DAG
  tourne en `max_active_runs=1` et toutes les tâches qui touchent la base
  (chargement, dbt, contrôle) passent par le pool `duckdb_writer` (1 slot),
  défini dans `airflow_settings.yaml`.
* **Aucune logique ici.** Le DAG câble des fonctions de `ingestion.pipeline`,
  testées hors Airflow. C'est aussi ce qui rend la migration BigQuery indolore :
  seul le loader change.

Le temps réel est une donnée provisoire : `eco2mix_monthly_consolidation` charge
en parallèle la version consolidée puis définitive publiée par RTE.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from ingestion.datasets import NATIONAL_TR
from ingestion.pipeline import check_freshness, extract_to_parquet, load_to_warehouse

#: Sérialise tous les accès au fichier DuckDB, y compris entre DAGs différents.
DUCKDB_WRITER_POOL = "duckdb_writer"

#: dbt tourne dans son propre environnement virtuel (voir airflow/Dockerfile).
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_BUILD_COMMAND = f"/usr/local/airflow/dbt_venv/bin/dbt build --project-dir {DBT_PROJECT_DIR}"


@dag(
    dag_id="eco2mix_hourly_ingest",
    description="Extrait éCO2mix national temps réel vers Parquet puis DuckDB",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["eco2mix", "ingestion", "duckdb", "dbt"],
    doc_md=__doc__,
)
def eco2mix_hourly_ingest() -> None:
    @task
    def extract() -> dict[str, Any]:
        """Appelle l'API ODRÉ et écrit un Parquet typé dans `data/bronze/`."""
        run_id = get_current_context()["run_id"]
        return asdict(extract_to_parquet(spec=NATIONAL_TR, run_id=run_id))

    @task(pool=DUCKDB_WRITER_POOL)
    def load_bronze(outcome: dict[str, Any]) -> dict[str, Any]:
        """Fusionne le Parquet dans `bronze.national_tr` sur la clé `date_heure`."""
        return load_to_warehouse(outcome["parquet_path"], spec=NATIONAL_TR).as_dict()

    # Reconstruit silver puis gold et joue les tests dbt. Dans le pool : dbt
    # écrit dans le même fichier DuckDB que `load_bronze`.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD_COMMAND,
        pool=DUCKDB_WRITER_POOL,
    )

    @task(pool=DUCKDB_WRITER_POOL)
    def freshness_check() -> str:
        """Échoue si la donnée la plus récente a plus de 2 h de retard.

        La lecture est en `read_only`, mais reste dans le pool : DuckDB refuse un
        lecteur externe tant qu'un writer détient le fichier.
        """
        return check_freshness(spec=NATIONAL_TR).latest.isoformat()

    load_bronze(extract()) >> dbt_build >> freshness_check()


eco2mix_hourly_ingest()
