"""Point d'entrée en ligne de commande, pour les rejeux manuels et le backfill.

    python -m ingestion.cli ingest                       # fenêtre glissante par défaut
    python -m ingestion.cli ingest --hours 24            # rattrapage d'une journée
    python -m ingestion.cli ingest --start 2025-01-01 --end 2026-01-01
    python -m ingestion.cli consolidate --months 24      # dataset consolidé/définitif
    python -m ingestion.cli load data/bronze/.../part-x.parquet
    python -m ingestion.cli freshness
    python -m ingestion.cli coverage
    python -m ingestion.cli geocode --adresse "12 rue de la Paix, 69003 Lyon"
    python -m ingestion.cli tempo --month 2026-09        # calendrier Tempo (API RTE)
    python -m ingestion.cli report --month 2026-09       # rapport PDF du mois
    python -m ingestion.cli report --month 2026-09 --format md   # même rapport en Markdown

Les mêmes fonctions sont appelées par les DAGs : ce qui tourne en production est
exactement ce qui tourne à la main.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from ingestion.config import get_settings
from ingestion.datasets import NATIONAL_CONS_DEF, NATIONAL_TR, SPECS, get_spec
from ingestion.extract_odre import ExtractionWindow
from ingestion.extract_rte import DayRange, RteCredentialsMissingError
from ingestion.geocode import GeocodingError, geocode
from ingestion.pipeline import (
    IncompleteMonthError,
    StaleDataError,
    check_freshness,
    check_month_coverage,
    extract_to_parquet,
    load_to_warehouse,
    run_consolidation,
    run_ingestion,
    run_tempo_ingestion,
    tempo_days_for_month,
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


def _parse_month(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"mois invalide : {value!r} (AAAA-MM)") from None


def _add_dataset_option(parser: argparse.ArgumentParser, default: str) -> None:
    parser.add_argument("--dataset", choices=sorted(SPECS), default=default, help="dataset ODRÉ")


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
        _add_dataset_option(sub, NATIONAL_TR.dataset_id)

    consolidate = subparsers.add_parser(
        "consolidate", help="relit les N derniers mois du dataset consolidé/définitif"
    )
    consolidate.add_argument("--months", type=int, help="profondeur en mois civils")
    consolidate.add_argument("--run-id", default="manual", help="suffixe du fichier Parquet")
    _add_dataset_option(consolidate, NATIONAL_CONS_DEF.dataset_id)

    load = subparsers.add_parser("load", help="charge un Parquet déjà extrait")
    load.add_argument("parquet_path", help="chemin du fichier Parquet")
    _add_dataset_option(load, NATIONAL_TR.dataset_id)

    freshness = subparsers.add_parser(
        "freshness", help="contrôle le retard de la donnée la plus récente"
    )
    _add_dataset_option(freshness, NATIONAL_TR.dataset_id)

    geocode_cmd = subparsers.add_parser(
        "geocode", help="résout une adresse en territoire administratif"
    )
    geocode_cmd.add_argument(
        "--adresse", required=True, help="adresse à résoudre, entre guillemets"
    )

    coverage = subparsers.add_parser(
        "coverage", help="contrôle la complétude du dernier mois révolu"
    )
    _add_dataset_option(coverage, NATIONAL_CONS_DEF.dataset_id)

    tempo = subparsers.add_parser("tempo", help="charge le calendrier Tempo (API RTE)")
    tempo.add_argument("--month", type=_parse_month, help="mois AAAA-MM (veille du 1er comprise)")
    tempo.add_argument("--start", type=date.fromisoformat, help="premier jour (inclus)")
    tempo.add_argument("--end", type=date.fromisoformat, help="dernier jour (exclu)")
    tempo.add_argument("--run-id", default="manual", help="suffixe du fichier Parquet")

    report = subparsers.add_parser("report", help="génère le rapport d'un mois (PDF ou Markdown)")
    report.add_argument("--month", type=_parse_month, required=True, help="mois AAAA-MM")
    report.add_argument("--format", choices=["pdf", "md"], default="pdf", help="format du rapport")
    report.add_argument("--output", help="chemin du fichier (défaut : data/reports/)")
    report.add_argument(
        "--sans-graphiques",
        action="store_true",
        help="Markdown uniquement : texte et tableaux, sans images PNG",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    settings = get_settings()
    spec = get_spec(args.dataset) if hasattr(args, "dataset") else None

    match args.command:
        case "extract":
            outcome = extract_to_parquet(
                settings=settings, spec=spec, window=_window_from(args), run_id=args.run_id
            )
            logger.info("extraction terminee %s", outcome)
        case "ingest":
            outcome, result = run_ingestion(
                settings=settings, spec=spec, window=_window_from(args), run_id=args.run_id
            )
            logger.info("ingestion terminee %s %s", outcome, result.as_dict())
        case "consolidate":
            outcome, result = run_consolidation(
                settings=settings, spec=spec, months=args.months, run_id=args.run_id
            )
            logger.info("consolidation terminee %s %s", outcome, result.as_dict())
        case "load":
            result = load_to_warehouse(args.parquet_path, settings=settings, spec=spec)
            logger.info("chargement %s", result)
        case "freshness":
            try:
                check_freshness(settings=settings, spec=spec)
            except StaleDataError as error:
                logger.error("donnee obsolete : %s", error)
                return 1
        case "coverage":
            try:
                check_month_coverage(settings=settings, spec=spec)
            except IncompleteMonthError as error:
                logger.error("mois incomplet : %s", error)
                return 1
        case "geocode":
            try:
                territoire = geocode(args.adresse, settings=settings)
            except GeocodingError as error:
                logger.error("geocodage impossible : %s", error)
                return 1
            lignes = [
                f"adresse           : {territoire.adresse}",
                f"commune           : {territoire.commune} ({territoire.code_insee_commune})",
                f"departement       : {territoire.departement} ({territoire.code_departement})",
                f"region            : {territoire.region}"
                f" ({territoire.code_insee_region or 'code inconnu'})",
                f"coordonnees       : {territoire.latitude:.5f}, {territoire.longitude:.5f}",
                f"precision / score : {territoire.precision} / {territoire.score:.2f}",
            ]
            print("\n".join(lignes))
        case "tempo":
            if args.month:
                days = tempo_days_for_month(args.month)
            elif args.start and args.end:
                days = DayRange(args.start, args.end)
            else:
                raise SystemExit("--month, ou --start et --end")
            try:
                outcome, result = run_tempo_ingestion(days, settings=settings, run_id=args.run_id)
            except RteCredentialsMissingError as error:
                logger.error("%s", error)
                return 1
            logger.info("tempo charge %s %s", outcome, result.as_dict())
        case "report":
            # Import paresseux : matplotlib et reportlab ne servent qu'ici.
            from reporting.monthly import generate_monthly_report

            output = Path(args.output) if args.output else None
            path = generate_monthly_report(
                args.month,
                settings=settings,
                output=output,
                format=args.format,
                graphiques=not args.sans_graphiques,
            )
            logger.info("rapport ecrit %s", path)
    return 0


if __name__ == "__main__":  # pragma: no cover - point d'entrée
    sys.exit(main())
