"""Consolidation mensuelle du mix électrique français (éCO2mix consolidé/définitif).

`extract` -> `load_bronze` -> `dbt_build` -> `coverage_check`

RTE publie deux qualités de données, et le temps réel n'est que la première :

| Dataset | Nature | Délai observé |
|---|---|---|
| `eco2mix-national-tr` | temps réel, révisé en continu | quelques minutes |
| `eco2mix-national-cons-def` | consolidées puis définitives | ~3 mois, puis rejeu annuel |

Ce DAG alimente la seconde. Trois conséquences de conception :

* **Fenêtre glissante de 24 mois.** Le consolidé arrive avec plusieurs mois de
  retard, puis une année entière bascule en « définitives » — les valeurs
  changent. Relire large et fusionner est la seule façon de rester juste, et le
  MERGE sur `date_heure` rend le recouvrement gratuit.
* **Un seul appel API.** Ce volume (~70 000 lignes) dépasse la profondeur de
  pagination de `/records` : le client bascule automatiquement sur
  `/exports/csv`, qui ramène la fenêtre entière en une requête.
* **Table distincte.** Le consolidé atterrit dans `bronze.national_cons_def`, à
  côté de `bronze.national_tr` : la couche bronze reste le miroir fidèle de
  chaque source, et c'est la couche silver (`silver.mix_unifie`, dbt) qui
  tranche la priorité consolidé/définitif > temps réel.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from ingestion.datasets import NATIONAL_CONS_DEF
from ingestion.pipeline import check_month_coverage, last_months_window, load_to_warehouse
from ingestion.pipeline import extract_to_parquet as extract_dataset

#: Sérialise tous les accès au fichier DuckDB, y compris entre DAGs différents.
DUCKDB_WRITER_POOL = "duckdb_writer"

#: dbt tourne dans son propre environnement virtuel (voir airflow/Dockerfile).
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_BUILD_COMMAND = f"/usr/local/airflow/dbt_venv/bin/dbt build --project-dir {DBT_PROJECT_DIR}"


@dag(
    dag_id="eco2mix_monthly_consolidation",
    description="Relit les 24 derniers mois du dataset éCO2mix consolidé/définitif",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": True,
    },
    tags=["eco2mix", "consolidation", "duckdb", "dbt"],
    doc_md=__doc__,
)
def eco2mix_monthly_consolidation() -> None:
    @task
    def extract() -> dict[str, Any]:
        """Exporte les 24 derniers mois consolidés en un seul appel API."""
        context = get_current_context()
        return asdict(
            extract_dataset(
                spec=NATIONAL_CONS_DEF,
                window=last_months_window(
                    # `catchup=False` : la fenêtre est ancrée sur la date logique
                    # du run, pas sur l'heure d'exécution, pour rester rejouable.
                    _lookback_months(),
                    now=context["data_interval_end"],
                ),
                run_id=context["run_id"],
            )
        )

    @task(pool=DUCKDB_WRITER_POOL)
    def load_bronze(outcome: dict[str, Any]) -> dict[str, Any]:
        """Fusionne dans `bronze.national_cons_def` sur la clé `date_heure`."""
        return load_to_warehouse(outcome["parquet_path"], spec=NATIONAL_CONS_DEF).as_dict()

    # Reconstruit silver (arbitrage consolidé > temps réel) puis gold. Dans le
    # pool : dbt écrit dans le même fichier DuckDB que `load_bronze`.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD_COMMAND,
        pool=DUCKDB_WRITER_POOL,
    )

    @task(pool=DUCKDB_WRITER_POOL)
    def coverage_check() -> dict[str, Any]:
        """Échoue si le dernier mois révolu a trop de pas de temps manquants.

        Contrôler la fraîcheur n'aurait ici aucun sens — le consolidé a
        structurellement plusieurs mois de retard. C'est la complétude qui compte.
        """
        return check_month_coverage(spec=NATIONAL_CONS_DEF).as_dict()

    load_bronze(extract()) >> dbt_build >> coverage_check()


def _lookback_months() -> int:
    """Profondeur configurée, lue à l'exécution et non à l'import du DAG."""
    from ingestion.config import get_settings

    return get_settings().consolidation_lookback_months


eco2mix_monthly_consolidation()
