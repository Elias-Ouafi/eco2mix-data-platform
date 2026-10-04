"""Point d'entrée du rapport mensuel, appelé par le DAG et par la CLI.

Deux formats, mêmes chiffres : `pdf` (mise en page imprimable) et `md`
(document Markdown, graphiques en PNG dans un dossier voisin).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import duckdb

from ingestion.config import Settings, get_settings
from reporting.data import Options, collect

logger = logging.getLogger(__name__)

Format = Literal["pdf", "md"]
FORMATS: tuple[Format, ...] = ("pdf", "md")


def parse_month(value: str) -> date:
    """`2026-09` -> 1er septembre 2026."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise ValueError(f"mois invalide : {value!r} (format attendu AAAA-MM)") from None


def report_path(mois: date, *, settings: Settings | None = None, format: Format = "pdf") -> Path:
    settings = settings or get_settings()
    return settings.reports_dir / f"eco2mix_rapport_{mois:%Y-%m}.{format}"


def generate_monthly_report(
    mois: date,
    *,
    settings: Settings | None = None,
    options: Options | None = None,
    output: Path | None = None,
    format: Format = "pdf",
    graphiques: bool = True,
    now: datetime | None = None,
) -> Path:
    """Génère le rapport d'un mois à partir de la couche gold, en lecture seule.

    `graphiques` ne concerne que le Markdown : le PDF embarque toujours les siens.
    """
    if format not in FORMATS:
        raise ValueError(f"format inconnu : {format!r} (attendu : {', '.join(FORMATS)})")
    settings = settings or get_settings()
    connection = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        rapport = collect(connection, mois, options, now=now)
    finally:
        connection.close()

    chemin = output or report_path(mois, settings=settings, format=format)
    # Imports à la demande : reportlab n'est utile qu'au PDF, matplotlib qu'aux graphiques.
    if format == "md":
        from reporting import markdown

        markdown.rendre(rapport, chemin, graphiques=graphiques)
    else:
        from reporting import pdf

        pdf.rendre(rapport, chemin)

    logger.info(
        "rapport genere format=%s mois=%s heures=%d/%d jours_tempo=%d/%d prix=%s path=%s",
        format,
        f"{mois:%Y-%m}",
        rapport.heures_completes,
        rapport.heures_attendues,
        rapport.jours_tempo_connus,
        rapport.jours_du_mois,
        rapport.prix_disponibles,
        chemin,
    )
    return chemin
