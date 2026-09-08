"""Étapes du pipeline, appelables depuis le DAG comme depuis la ligne de commande.

Le DAG Airflow ne contient que du câblage : toute la logique vit ici, ce qui la
rend testable sans Airflow et rejouable à la main en cas d'incident.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.config import Settings, get_settings
from ingestion.extract_odre import ExtractionWindow, OdreClient
from ingestion.transform import records_to_table, write_parquet
from ingestion.warehouse import LoadResult, build_loader

logger = logging.getLogger(__name__)


class StaleDataError(RuntimeError):
    """La donnée la plus récente du warehouse dépasse le retard toléré."""


@dataclass(frozen=True, slots=True)
class ExtractionOutcome:
    """Résultat d'une extraction, remonté en XCom par le DAG."""

    parquet_path: str
    rows: int
    api_calls: int
    window_start: str
    window_end: str


#: Colonne témoin du contrôle de fraîcheur. RTE publie l'horodatage le plus
#: récent quelques minutes avant ses mesures : compter cette ligne « à blanc »
#: rendrait le contrôle vert alors que plus aucune valeur n'arrive.
FRESHNESS_MEASURE = "consommation"


@dataclass(frozen=True, slots=True)
class FreshnessReport:
    """Écart entre la donnée la plus récente et l'instant du contrôle."""

    #: Dernier horodatage effectivement mesuré : c'est lui qui décide.
    latest: datetime | None
    #: Dernier horodatage présent, mesuré ou non. Informatif.
    latest_row: datetime | None
    lag_hours: float | None
    max_lag_hours: int
    row_count: int

    @property
    def is_fresh(self) -> bool:
        return self.lag_hours is not None and self.lag_hours <= self.max_lag_hours


def extract_to_parquet(
    *,
    settings: Settings | None = None,
    window: ExtractionWindow | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> ExtractionOutcome:
    """Extrait une fenêtre de l'API et l'écrit en Parquet typé.

    Un fichier est écrit même si la fenêtre est vide : le chargement en aval
    reste uniforme, et c'est le contrôle de fraîcheur — pas l'extraction — qui
    décide si l'absence de donnée est anormale.
    """
    settings = settings or get_settings()
    ingested_at = (now or datetime.now(UTC)).astimezone(UTC)
    window = window or ExtractionWindow.last_hours(settings.lookback_hours, now=ingested_at)

    with OdreClient(settings) as client:
        records = client.fetch_window(window)
        api_calls = client.calls.count

    if not records:
        logger.warning("aucun enregistrement publie sur la fenetre %s", window)

    table = records_to_table(records, dataset_id=settings.dataset_id, ingested_at=ingested_at)
    path = write_parquet(
        table,
        raw_dataset_dir=settings.raw_dataset_dir,
        run_id=run_id,
        ingest_date=ingested_at.date(),
    )
    return ExtractionOutcome(
        parquet_path=str(path),
        rows=table.num_rows,
        api_calls=api_calls,
        window_start=window.start.isoformat(),
        window_end=window.end.isoformat(),
    )


def load_to_warehouse(parquet_path: Path | str, *, settings: Settings | None = None) -> LoadResult:
    """Fusionne un Parquet dans le warehouse configuré, sur la clé `date_heure`."""
    settings = settings or get_settings()
    loader = build_loader(settings)
    try:
        return loader.merge_parquet(parquet_path)
    finally:
        loader.close()


def check_freshness(
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> FreshnessReport:
    """Vérifie le retard de la donnée la plus récente, en lecture seule.

    Lève `StaleDataError` si le warehouse est vide ou en retard : c'est ce qui
    fait échouer la tâche Airflow et déclenche l'alerte.
    """
    settings = settings or get_settings()
    reference = (now or datetime.now(UTC)).astimezone(UTC)

    # La lecture seule évite de prendre le verrou d'écriture unique de DuckDB.
    loader = build_loader(settings, read_only=True)
    try:
        latest = loader.latest_timestamp(measure=FRESHNESS_MEASURE)
        latest_row = loader.latest_timestamp()
        row_count = loader.row_count()
    finally:
        loader.close()

    lag = None if latest is None else (reference - latest) / timedelta(hours=1)
    report = FreshnessReport(
        latest=latest,
        latest_row=latest_row,
        lag_hours=None if lag is None else round(lag, 3),
        max_lag_hours=settings.freshness_max_lag_hours,
        row_count=row_count,
    )

    if latest is None:
        raise StaleDataError(
            "le warehouse ne contient aucune donnee"
            if row_count == 0
            else f"{row_count} ligne(s) presentes mais aucune valeur de {FRESHNESS_MEASURE}"
        )
    if not report.is_fresh:
        raise StaleDataError(
            f"donnee la plus recente {latest.isoformat()} "
            f"(retard {report.lag_hours} h > {report.max_lag_hours} h tolerees)"
        )

    logger.info(
        "freshness ok latest_measured=%s latest_row=%s lag_hours=%s rows=%d",
        latest.isoformat(),
        None if latest_row is None else latest_row.isoformat(),
        report.lag_hours,
        row_count,
    )
    return report


def run_ingestion(
    *,
    settings: Settings | None = None,
    window: ExtractionWindow | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> tuple[ExtractionOutcome, LoadResult]:
    """Extraction puis chargement, pour un rejeu manuel en une commande."""
    settings = settings or get_settings()
    outcome = extract_to_parquet(settings=settings, window=window, run_id=run_id, now=now)
    result = load_to_warehouse(outcome.parquet_path, settings=settings)
    return outcome, result
