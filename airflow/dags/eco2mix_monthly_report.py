"""Rapport mensuel à la demande : pics de carbone et meilleurs créneaux Tempo.

`resolve_month` -> `extract_tempo` -> `load_tempo` -> `dbt_build` -> `generate_report`

DAG **sans planification** : il se déclenche depuis l'UI (« Trigger DAG w/ config »)
avec le mois voulu. Le PDF est écrit dans `data/reports/eco2mix_rapport_AAAA-MM.pdf`.

| Paramètre | Défaut | Rôle |
|---|---|---|
| `mois` | mois précédent | Mois du rapport, `AAAA-MM` |
| `tempo` | `true` | Recharge le calendrier Tempo du mois depuis l'API RTE |
| `heures_creneau` | `3` | Durée des créneaux recommandés |
| `usage_flexible_kwh` | `7` | Consommation déplaçable par jour, pour l'estimation d'économie |

* **Prix Tempo.** La couleur des jours vient de l'API RTE, qui exige des
  identifiants (`ECO2MIX_RTE_CLIENT_ID` / `ECO2MIX_RTE_CLIENT_SECRET`). Sans eux, la
  tâche `extract_tempo` échoue explicitement ; `tempo=false` produit un rapport
  carbone seul, sans prix, en le signalant dans le PDF.
* **Données éCO2mix.** Ce DAG ne les extrait pas : elles sont déjà dans la couche
  gold grâce aux DAGs horaire et mensuel. Le rapport indique sa couverture réelle.
* **Un seul writer.** Le chargement, dbt et la lecture du rapport passent par le
  pool `duckdb_writer`, comme dans les autres DAGs.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from ingestion.datasets import RTE_TEMPO
from ingestion.pipeline import extract_tempo_to_parquet, load_to_warehouse, tempo_days_for_month

#: Sérialise tous les accès au fichier DuckDB, y compris entre DAGs différents.
DUCKDB_WRITER_POOL = "duckdb_writer"

#: dbt tourne dans son propre environnement virtuel (voir airflow/Dockerfile).
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_BUILD_COMMAND = f"/usr/local/airflow/dbt_venv/bin/dbt build --project-dir {DBT_PROJECT_DIR}"


@dag(
    dag_id="eco2mix_monthly_report",
    description="Rapport PDF mensuel : pics de carbone et meilleurs créneaux Tempo",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "mois": Param(
            "",
            type="string",
            pattern=r"^(\d{4}-(0[1-9]|1[0-2]))?$",
            description="Mois du rapport (AAAA-MM). Vide : mois précédent.",
        ),
        "tempo": Param(True, type="boolean", description="Recharger le calendrier Tempo"),
        "heures_creneau": Param(3, type="integer", minimum=1, maximum=8),
        "usage_flexible_kwh": Param(7.0, type="number", minimum=0),
    },
    tags=["eco2mix", "rapport", "tempo"],
    doc_md=__doc__,
)
def eco2mix_monthly_report() -> None:
    @task
    def resolve_month() -> str:
        """Mois demandé, ou à défaut le mois précédant le déclenchement."""
        context = get_current_context()
        requested = context["params"]["mois"]
        if requested:
            return requested
        logical = context["logical_date"].in_timezone("Europe/Paris")
        previous = logical.start_of("month").subtract(days=1)
        return previous.format("YYYY-MM")

    @task
    def extract_tempo(month: str) -> dict[str, Any]:
        """Calendrier Tempo du mois (veille du 1er comprise) vers un Parquet bronze."""
        context = get_current_context()
        if not context["params"]["tempo"]:
            raise AirflowSkipException("tempo=false : rapport sans prix")
        days = tempo_days_for_month(date.fromisoformat(f"{month}-01"))
        return asdict(extract_tempo_to_parquet(days, run_id=context["run_id"]))

    @task(pool=DUCKDB_WRITER_POOL)
    def load_tempo(outcome: dict[str, Any]) -> dict[str, Any]:
        """Fusionne le calendrier dans `bronze.rte_tempo`."""
        return load_to_warehouse(outcome["parquet_path"], spec=RTE_TEMPO).as_dict()

    # `none_failed` : s'exécute aussi quand le chargement Tempo est volontairement sauté.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD_COMMAND,
        pool=DUCKDB_WRITER_POOL,
        trigger_rule="none_failed",
    )

    @task(pool=DUCKDB_WRITER_POOL)
    def generate_report(month: str) -> str:
        """Écrit le PDF à partir de la couche gold, en lecture seule."""
        # Import à l'exécution : matplotlib et reportlab alourdiraient le parsing du DAG.
        from reporting.data import Options
        from reporting.monthly import generate_monthly_report

        params = get_current_context()["params"]
        options = Options(
            heures_creneau=int(params["heures_creneau"]),
            usage_flexible_kwh=float(params["usage_flexible_kwh"]),
        )
        return str(generate_monthly_report(date.fromisoformat(f"{month}-01"), options=options))

    month = resolve_month()
    load_tempo(extract_tempo(month)) >> dbt_build >> generate_report(month)


eco2mix_monthly_report()
