"""Étapes du pipeline, appelables depuis les DAGs comme depuis la ligne de commande.

Les DAGs Airflow ne contiennent que du câblage : toute la logique vit ici, ce qui
la rend testable sans Airflow et rejouable à la main en cas d'incident. Chaque
étape est paramétrée par un `DatasetSpec`, si bien que les deux flux — temps réel
horaire et consolidé mensuel — partagent exactement le même code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ingestion.config import Settings, get_settings
from ingestion.datasets import BRONZE_SPECS, NATIONAL_TR, RTE_TEMPO, DatasetSpec
from ingestion.extract_odre import ExtractionWindow, OdreClient
from ingestion.extract_rte import DayRange, RteTempoClient
from ingestion.transform import PARIS_TZ, records_to_table, write_parquet
from ingestion.warehouse import LoadResult, build_loader

logger = logging.getLogger(__name__)

#: Colonne témoin du contrôle de fraîcheur. RTE publie l'horodatage le plus
#: récent quelques minutes avant ses mesures : compter cette ligne « à blanc »
#: rendrait le contrôle vert alors que plus aucune valeur n'arrive.
FRESHNESS_MEASURE = "consommation"

#: Pas de publication des datasets éCO2mix.
SLOT_MINUTES = 15


class StaleDataError(RuntimeError):
    """La donnée la plus récente du warehouse dépasse le retard toléré."""


class IncompleteMonthError(RuntimeError):
    """Le dernier mois consolidé disponible comporte trop de pas de temps manquants."""


@dataclass(frozen=True, slots=True)
class ExtractionOutcome:
    """Résultat d'une extraction, remonté en XCom par les DAGs."""

    dataset_id: str
    parquet_path: str
    rows: int
    api_calls: int
    window_start: str
    window_end: str


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


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Complétude d'un mois civil dans la table consolidée."""

    month: str
    expected_slots: int
    actual_slots: int
    min_coverage: float

    @property
    def coverage(self) -> float:
        return self.actual_slots / self.expected_slots if self.expected_slots else 0.0

    @property
    def is_complete(self) -> bool:
        return self.coverage >= self.min_coverage

    def as_dict(self) -> dict[str, float | int | str]:
        return {
            "month": self.month,
            "expected_slots": self.expected_slots,
            "actual_slots": self.actual_slots,
            "coverage": round(self.coverage, 4),
        }


# --- Fenêtres --------------------------------------------------------------


def month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    """Bornes UTC `[début, fin)` d'un mois civil français.

    Passe par `Europe/Paris` : un mois contenant un changement d'heure ne dure
    pas un nombre entier de jours de 24 h, et c'est précisément ce décalage qui
    doit être reflété dans le nombre de pas de temps attendus.
    """
    start = datetime(year, month, 1, tzinfo=PARIS_TZ)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    end = datetime(next_year, next_month, 1, tzinfo=PARIS_TZ)
    return start.astimezone(UTC), end.astimezone(UTC)


def last_months_window(months: int, *, now: datetime | None = None) -> ExtractionWindow:
    """Fenêtre couvrant les `months` derniers mois civils, jusqu'à maintenant."""
    reference = (now or datetime.now(UTC)).astimezone(PARIS_TZ)
    shifted = (reference.year * 12 + reference.month - 1) - months
    start, _ = month_bounds(shifted // 12, shifted % 12 + 1)
    return ExtractionWindow(start=start, end=reference.astimezone(UTC))


def _expected_slots(start: datetime, end: datetime) -> int:
    return int((end - start) / timedelta(minutes=SLOT_MINUTES))


# --- Étapes ----------------------------------------------------------------


def extract_to_parquet(
    *,
    settings: Settings | None = None,
    spec: DatasetSpec = NATIONAL_TR,
    window: ExtractionWindow | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> ExtractionOutcome:
    """Extrait une fenêtre de l'API et l'écrit en Parquet typé.

    Un fichier est écrit même si la fenêtre est vide : le chargement en aval
    reste uniforme, et ce sont les contrôles qualité — pas l'extraction — qui
    décident si l'absence de donnée est anormale.
    """
    settings = settings or get_settings()
    ingested_at = (now or datetime.now(UTC)).astimezone(UTC)
    window = window or ExtractionWindow.last_hours(settings.lookback_hours, now=ingested_at)

    with OdreClient(settings, dataset_id=spec.dataset_id) as client:
        records = client.fetch_window(window)
        api_calls = client.calls.count

    if not records:
        logger.warning("aucun enregistrement publie sur la fenetre %s", window)

    table = records_to_table(records, spec=spec, ingested_at=ingested_at)
    path = write_parquet(
        table,
        dataset_dir=spec.bronze_dataset_dir(settings.bronze_dir),
        run_id=run_id,
        ingest_date=ingested_at.date(),
    )
    return ExtractionOutcome(
        dataset_id=spec.dataset_id,
        parquet_path=str(path),
        rows=table.num_rows,
        api_calls=api_calls,
        window_start=window.start.isoformat(),
        window_end=window.end.isoformat(),
    )


def tempo_days_for_month(month: date) -> DayRange:
    """Jours Tempo utiles au rapport d'un mois, veille du 1er comprise.

    Les heures de 0 h à 6 h du 1er appartiennent au jour Tempo de la veille.
    """
    days = DayRange.month(month.year, month.month)
    return DayRange(days.start - timedelta(days=1), days.end)


def extract_tempo_to_parquet(
    days: DayRange,
    *,
    settings: Settings | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> ExtractionOutcome:
    """Extrait le calendrier Tempo d'une plage de jours vers un Parquet bronze."""
    settings = settings or get_settings()
    ingested_at = (now or datetime.now(UTC)).astimezone(UTC)

    with RteTempoClient(settings) as client:
        records = client.fetch_calendar(days, today=ingested_at.astimezone(PARIS_TZ).date())
        api_calls = client.calls

    if not records:
        logger.warning("aucune couleur Tempo publiee sur %s", days)

    table = records_to_table(records, spec=RTE_TEMPO, ingested_at=ingested_at)
    path = write_parquet(
        table,
        dataset_dir=RTE_TEMPO.bronze_dataset_dir(settings.bronze_dir),
        run_id=run_id,
        ingest_date=ingested_at.date(),
    )
    return ExtractionOutcome(
        dataset_id=RTE_TEMPO.dataset_id,
        parquet_path=str(path),
        rows=table.num_rows,
        api_calls=api_calls,
        window_start=days.start.isoformat(),
        window_end=days.end.isoformat(),
    )


def ensure_bronze_tables(*, settings: Settings | None = None) -> None:
    """Crée, vides, les tables bronze de tous les datasets connus.

    La couche silver (dbt) lit toutes les sources bronze à chaque run. Sans
    cela, le premier run horaire échouerait tant que le DAG mensuel n'a pas
    créé `bronze.national_cons_def` — une dépendance d'ordre de déploiement
    qu'aucun des deux DAGs ne devrait porter.
    """
    settings = settings or get_settings()
    for spec in BRONZE_SPECS:
        loader = build_loader(settings, spec=spec)
        try:
            loader.ensure_table()
        finally:
            loader.close()


def load_to_warehouse(
    parquet_path: Path | str,
    *,
    settings: Settings | None = None,
    spec: DatasetSpec = NATIONAL_TR,
) -> LoadResult:
    """Fusionne un Parquet dans la table bronze du dataset, sur la clé `date_heure`."""
    settings = settings or get_settings()
    ensure_bronze_tables(settings=settings)
    loader = build_loader(settings, spec=spec)
    try:
        return loader.merge_parquet(parquet_path)
    finally:
        loader.close()


def check_freshness(
    *,
    settings: Settings | None = None,
    spec: DatasetSpec = NATIONAL_TR,
    now: datetime | None = None,
) -> FreshnessReport:
    """Vérifie le retard de la donnée la plus récente, en lecture seule.

    Lève `StaleDataError` si le warehouse est vide ou en retard : c'est ce qui
    fait échouer la tâche Airflow et déclenche l'alerte.
    """
    settings = settings or get_settings()
    reference = (now or datetime.now(UTC)).astimezone(UTC)

    # La lecture seule évite de prendre le verrou d'écriture unique de DuckDB.
    loader = build_loader(settings, spec=spec, read_only=True)
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


def check_month_coverage(
    *,
    settings: Settings | None = None,
    spec: DatasetSpec,
    now: datetime | None = None,
) -> CoverageReport:
    """Vérifie qu'un mois consolidé est complet, en lecture seule.

    Le contrôle porte sur le dernier mois civil **révolu** présent en base : un
    mois en cours de publication serait forcément incomplet. Le nombre de pas de
    temps attendus est calculé sur les bornes réelles du mois en heure de Paris,
    donc un mois de changement d'heure attend 4 pas de moins (mars) ou de plus
    (octobre) — un comptage naïf en jours de 24 h ferait échouer mars à tort.

    Lève `IncompleteMonthError` sous le seuil de couverture configuré.
    """
    settings = settings or get_settings()
    reference = (now or datetime.now(UTC)).astimezone(PARIS_TZ)

    loader = build_loader(settings, spec=spec, read_only=True)
    try:
        latest = loader.latest_timestamp()
        if latest is None:
            raise IncompleteMonthError(f"{spec.table_name} ne contient aucune donnee")

        year, month = _target_month(latest, reference)
        start, end = month_bounds(year, month)
        actual = loader.count_distinct_keys(start=start, end=end)
    finally:
        loader.close()

    report = CoverageReport(
        month=f"{year:04d}-{month:02d}",
        expected_slots=_expected_slots(start, end),
        actual_slots=actual,
        min_coverage=settings.consolidation_min_coverage,
    )

    if not report.is_complete:
        raise IncompleteMonthError(
            f"mois {report.month} incomplet : {report.actual_slots}/{report.expected_slots} "
            f"pas de temps ({report.coverage:.1%} < {report.min_coverage:.0%})"
        )

    logger.info("coverage ok %s", report.as_dict())
    return report


def _target_month(latest: datetime, reference: datetime) -> tuple[int, int]:
    """Dernier mois civil révolu présent en base, exprimé en heure de Paris."""
    local = latest.astimezone(PARIS_TZ)
    year, month = local.year, local.month
    if (year, month) == (reference.year, reference.month):
        # Le mois courant n'est pas terminé : on juge le précédent.
        previous = (year * 12 + month - 1) - 1
        year, month = previous // 12, previous % 12 + 1
    return year, month


def run_ingestion(
    *,
    settings: Settings | None = None,
    spec: DatasetSpec = NATIONAL_TR,
    window: ExtractionWindow | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> tuple[ExtractionOutcome, LoadResult]:
    """Extraction puis chargement, pour un rejeu manuel en une commande."""
    settings = settings or get_settings()
    outcome = extract_to_parquet(
        settings=settings, spec=spec, window=window, run_id=run_id, now=now
    )
    result = load_to_warehouse(outcome.parquet_path, settings=settings, spec=spec)
    return outcome, result


def run_tempo_ingestion(
    days: DayRange,
    *,
    settings: Settings | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> tuple[ExtractionOutcome, LoadResult]:
    """Calendrier Tempo : extraction puis chargement dans `bronze.rte_tempo`."""
    settings = settings or get_settings()
    outcome = extract_tempo_to_parquet(days, settings=settings, run_id=run_id, now=now)
    result = load_to_warehouse(outcome.parquet_path, settings=settings, spec=RTE_TEMPO)
    return outcome, result


def run_consolidation(
    *,
    settings: Settings | None = None,
    spec: DatasetSpec,
    months: int | None = None,
    run_id: str = "manual",
    now: datetime | None = None,
) -> tuple[ExtractionOutcome, LoadResult]:
    """Relit les N derniers mois consolidés et les fusionne.

    La fenêtre est volontairement large : RTE publie le consolidé avec plusieurs
    mois de retard, puis rejoue une année entière lorsqu'elle bascule en
    « définitives ». Le volume déclenche automatiquement l'export CSV, donc ce
    recouvrement coûte **un seul appel API**.
    """
    settings = settings or get_settings()
    months = months if months is not None else settings.consolidation_lookback_months
    window = last_months_window(months, now=now)
    return run_ingestion(settings=settings, spec=spec, window=window, run_id=run_id, now=now)
