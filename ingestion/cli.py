"""Point d'entrée en ligne de commande, pour les rejeux manuels et le backfill.

    python -m ingestion.cli ingest                  # fenêtre glissante par défaut
    python -m ingestion.cli ingest --hours 24       # rattrapage d'une journée
    python -m ingestion.cli ingest --start 2025-01-01 --end 2026-01-01
    python -m ingestion.cli load data/raw/.../part-x.parquet
    python -m ingestion.cli freshness

Les mêmes fonctions sont appelées par le DAG : ce qui tourne en production est
exactement ce qui tourne à la main.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from ingestion.config import get_settings
from ingestion.extract_odre import ExtractionWindow
from ingestion.pipeline import (
    StaleDataError,
    check_freshness,
    extract_to_parquet,
    load_to_warehouse,
    run_ingestion,
)

logger = logging.getLogger("ingestion")


def _parse_instant(value: str) -> datetime:
    """Accepte `2026-09-07` ou `2026-09-07T05:00:00`, interprétés en UTC."""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _window_from(args: argparse.Namespace) -> ExtractionWindow | None:
    if args.start or args.end:
        if not (args.start and args.end):
            raise SystemExit("--start et --end vont par paire")
        return ExtractionWindow(start=args.start, end=args.end)
    if args.hours:
        return ExtractionWindow.last_hours(args.hours)
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eco2mix",
        description=__doc__,
        # Sans cela, argparse recompacte les exemples de la docstring en un pavé.
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", action="store_true", help="passe les logs en DEBUG")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("extract", "extrait une fenêtre vers un Parquet, sans charger"),
        ("ingest", "extrait puis charge dans le warehouse"),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("--hours", type=int, help="profondeur de la fenêtre glissante")
        sub.add_argument("--start", type=_parse_instant, help="borne de début (UTC, incluse)")
        sub.add_argument("--end", type=_parse_instant, help="borne de fin (UTC, exclue)")
        sub.add_argument("--run-id", default="manual", help="suffixe du fichier Parquet produit")

    load = subparsers.add_parser("load", help="charge un Parquet déjà extrait")
    load.add_argument("parquet_path", help="chemin du fichier Parquet")

    subparsers.add_parser("freshness", help="contrôle le retard de la donnée la plus récente")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    settings = get_settings()

    match args.command:
        case "extract":
            outcome = extract_to_parquet(
                settings=settings, window=_window_from(args), run_id=args.run_id
            )
            logger.info("extraction terminee %s", outcome)
        case "ingest":
            outcome, result = run_ingestion(
                settings=settings, window=_window_from(args), run_id=args.run_id
            )
            logger.info("ingestion terminee %s %s", outcome, result.as_dict())
        case "load":
            logger.info("chargement %s", load_to_warehouse(args.parquet_path, settings=settings))
        case "freshness":
            try:
                check_freshness(settings=settings)
            except StaleDataError as error:
                logger.error("donnee obsolete : %s", error)
                return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - point d'entrée
    sys.exit(main())
