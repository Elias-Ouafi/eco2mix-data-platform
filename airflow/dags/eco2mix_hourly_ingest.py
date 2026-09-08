"""Ingestion horaire du mix électrique français (éCO2mix national temps réel).

`extract` -> `load_raw` -> `dbt_build` (placeholder) -> `freshness_check`

* **Fenêtre glissante.** Chaque run relit les 3 dernières heures et non la seule
  heure écoulée : RTE révise ses valeurs temps réel après publication, et le
  MERGE sur `date_heure` rend ce recouvrement gratuit.
* **Un seul writer.** DuckDB n'accepte qu'une connexion en écriture. Le DAG
  tourne en `max_active_runs=1` et les tâches qui touchent la base passent par
  le pool `duckdb_writer` (1 slot), défini dans `airflow_settings.yaml`.
* **Aucune logique ici.** Le DAG câble des fonctions de `ingestion.pipeline`,
  testées hors Airflow. C'est aussi ce qui rend la migration BigQuery indolore :
  seul le loader change.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from ingestion.pipeline import check_freshness, extract_to_parquet, load_to_warehouse

#: Sérialise tous les accès au fichier DuckDB, y compris entre DAGs différents.
DUCKDB_WRITER_POOL = "duckdb_writer"


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
    tags=["eco2mix", "ingestion", "duckdb"],
    doc_md=__doc__,
)
def eco2mix_hourly_ingest() -> None:
    @task
    def extract() -> dict[str, Any]:
        """Appelle l'API ODRÉ et écrit un Parquet typé dans `data/raw/`."""
        run_id = get_current_context()["run_id"]
        return asdict(extract_to_parquet(run_id=run_id))

    @task(pool=DUCKDB_WRITER_POOL)
    def load_raw(outcome: dict[str, Any]) -> dict[str, Any]:
        """Fusionne le Parquet dans `raw.national_tr` sur la clé `date_heure`."""
        return load_to_warehouse(outcome["parquet_path"]).as_dict()

    # Emplacement réservé aux modèles dbt (staging + marts), hors périmètre de
    # cette itération. À remplacer par, au choix :
    #
    # dbt_build = BashOperator(
    #     task_id="dbt_build",
    #     bash_command="dbt build --project-dir /usr/local/airflow/dbt",
    # )
    dbt_build = EmptyOperator(task_id="dbt_build")

    @task(pool=DUCKDB_WRITER_POOL)
    def freshness_check() -> str:
        """Échoue si la donnée la plus récente a plus de 2 h de retard.

        La lecture est en `read_only`, mais reste dans le pool : DuckDB refuse un
        lecteur externe tant qu'un writer détient le fichier.
        """
        return check_freshness().latest.isoformat()

    load_raw(extract()) >> dbt_build >> freshness_check()


eco2mix_hourly_ingest()
