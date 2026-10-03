"""Point d'entrée du rapport mensuel, appelé par le DAG et par la CLI."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import duckdb

from ingestion.config import Settings, get_settings
from reporting import pdf
from reporting.data import Options, collect

logger = logging.getLogger(__name__)


def parse_month(value: str) -> date:
    """`2026-09` -> 1er septembre 2026."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise ValueError(f"mois invalide : {value!r} (format attendu AAAA-MM)") from None


def report_path(mois: date, *, settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    return settings.reports_dir / f"eco2mix_rapport_{mois:%Y-%m}.pdf"


def generate_monthly_report(
    mois: date,
    *,
    settings: Settings | None = None,
    options: Options | None = None,
    output: Path | None = None,
    now: datetime | None = None,
) -> Path:
    """Génère le PDF d'un mois à partir de la couche gold, en lecture seule."""
    settings = settings or get_settings()
    connection = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        rapport = collect(connection, mois, options, now=now)
    finally:
        connection.close()

    chemin = pdf.rendre(rapport, output or report_path(mois, settings=settings))
    logger.info(
        "rapport genere mois=%s heures=%d/%d jours_tempo=%d/%d prix=%s path=%s",
        f"{mois:%Y-%m}",
        rapport.heures_completes,
        rapport.heures_attendues,
        rapport.jours_tempo_connus,
        rapport.jours_du_mois,
        rapport.prix_disponibles,
        chemin,
    )
    return chemin
