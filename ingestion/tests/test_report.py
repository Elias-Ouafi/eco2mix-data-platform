"""Rapport mensuel : chiffres calculés sur la couche gold, puis rendu PDF.

S'appuie sur la base de `medallion.py` : le 30 juin 2026 est un jour rouge,
précédé d'un jour bleu, avec un pic de carbone à 19 h et 20 h (heure de Paris).
Suite ignorée si dbt, matplotlib ou reportlab ne sont pas installés.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from itertools import pairwise
from pathlib import Path

import duckdb
import pytest

from ingestion.config import Settings
from ingestion.tests.medallion import HEURES_PIC, PIC_CO2
from ingestion.transform import PARIS_TZ

pytest.importorskip("matplotlib", reason="groupe report non installé (uv sync --group report)")
pytest.importorskip("reportlab", reason="groupe report non installé (uv sync --group report)")

from reporting.data import Options, collect
from reporting.monthly import generate_monthly_report, parse_month, report_path

JUIN = date(2026, 6, 1)
NOW = datetime(2026, 7, 2, 9, 0, tzinfo=PARIS_TZ)


@pytest.fixture(scope="module")
def connection(medallion_db: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect(str(medallion_db), read_only=True)
    yield connection
    connection.close()


def test_carbon_peak_is_detected_as_one_episode(connection: duckdb.DuckDBPyConnection) -> None:
    rapport = collect(connection, JUIN, now=NOW)

    assert rapport.heure_plus_carbonee.taux_co2 == PIC_CO2
    premier = rapport.episodes[0]
    assert (premier.debut.hour, premier.fin.hour) == (HEURES_PIC[0], HEURES_PIC[-1] + 1)
    assert premier.duree_heures == 2
    assert premier.co2_max == PIC_CO2


def test_coverage_reflects_the_single_loaded_day(connection: duckdb.DuckDBPyConnection) -> None:
    rapport = collect(connection, JUIN, now=NOW)

    assert (rapport.heures_attendues, rapport.heures_completes) == (720, 24)
    assert rapport.jours_tempo_connus == 2
    assert rapport.repartition_tempo == {"bleu": 1, "blanc": 0, "rouge": 1}
    assert rapport.jours_rouges == [date(2026, 6, 30)]
    assert rapport.prix_disponibles


def test_best_window_is_the_cheapest_blue_off_peak(connection: duckdb.DuckDBPyConnection) -> None:
    rapport = collect(connection, JUIN, now=NOW)

    meilleur = rapport.meilleurs_creneaux[0]
    # 0 h–3 h le 30 juin : heures creuses du jour Tempo bleu de la veille.
    assert (meilleur.debut, meilleur.couleur, meilleur.periode) == (
        datetime(2026, 6, 30, 0, 0, tzinfo=PARIS_TZ),
        "bleu",
        "hc",
    )
    assert meilleur.prix_moyen == pytest.approx(0.1325)
    pire = rapport.pires_creneaux[0]
    assert (pire.couleur, pire.periode) == ("rouge", "hp")
    # Les créneaux retenus ne se chevauchent pas.
    creneaux = sorted(rapport.meilleurs_creneaux, key=lambda c: c.debut)
    assert all(a.fin <= b.debut for a, b in pairwise(creneaux))


def test_flexible_usage_savings(connection: duckdb.DuckDBPyConnection) -> None:
    rapport = collect(connection, JUIN, Options(usage_flexible_kwh=10), now=NOW)

    economie = rapport.economie
    assert economie.nb_jours == 1
    # Meilleur créneau : bleu HC à 0,1325 €. Référence 18 h–21 h : rouge HP à 0,7060 €.
    assert economie.cout_optimise == pytest.approx(10 * 0.1325)
    assert economie.cout_reference == pytest.approx(10 * 0.7060)
    # Référence : 18 h (20 g), 19 h et 20 h (pic à 80 g) -> 60 g/kWh de moyenne.
    assert economie.co2_reference_kg == pytest.approx(10 * 60 / 1000)
    assert economie.co2_optimise_kg == pytest.approx(10 * 20 / 1000)


def test_report_without_tempo_falls_back_to_carbon_only(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    # Octobre 2025 : couleurs connues, mais aucune grille tarifaire avant 2026.
    rapport = collect(connection, date(2025, 10, 1), now=NOW)

    assert rapport.heures_attendues == 745  # retour à l'heure d'hiver
    assert not rapport.prix_disponibles
    assert rapport.meilleurs_creneaux[0].prix_moyen is None
    assert rapport.economie.gain_euros is None


def test_pdf_is_written(medallion_db: Path, tmp_path: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(JUIN, settings=settings, now=NOW)

    assert chemin == report_path(JUIN, settings=settings)
    assert chemin.name == "eco2mix_rapport_2026-06.pdf"
    contenu = chemin.read_bytes()
    assert contenu.startswith(b"%PDF")
    assert len(contenu) > 50_000  # graphiques embarqués


def test_pdf_is_written_for_a_month_without_data(medallion_db: Path, tmp_path: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(date(2024, 1, 1), settings=settings, now=NOW)

    assert chemin.read_bytes().startswith(b"%PDF")


# --- Markdown ------------------------------------------------------------------


def test_markdown_report_carries_the_same_figures(medallion_db: Path, tmp_path: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(JUIN, settings=settings, format="md", now=NOW)

    assert chemin == report_path(JUIN, settings=settings, format="md")
    assert chemin.name == "eco2mix_rapport_2026-06.md"
    texte = chemin.read_text(encoding="utf-8")
    assert texte.startswith("# Électricité en France — juin 2026")
    sections = (
        "## En bref",
        "## 1. Pics de carbone",
        "## 2. Calendrier Tempo et prix",
        "## 3. Les meilleurs créneaux pour consommer",
        "## Méthodologie et sources",
    )
    assert all(section in texte for section in sections)
    # Pic à 80 g/kWh, jour rouge, meilleur créneau bleu HC à 0,1325 €/kWh.
    assert "80 g/kWh" in texte
    assert "**Jours rouges :** mardi 30 juin." in texte
    assert "| bleu | creuses |" in texte
    assert "0,1325 €/kWh" in texte


def test_markdown_charts_are_written_next_to_the_document(
    medallion_db: Path, tmp_path: Path
) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(JUIN, settings=settings, format="md", now=NOW)

    dossier = tmp_path / "eco2mix_rapport_2026-06_graphiques"
    images = sorted(p.name for p in dossier.glob("*.png"))
    assert images == [
        "calendrier_tempo.png",
        "carte_chaleur.png",
        "courbe_carbone.png",
        "profil_horaire.png",
    ]
    texte = chemin.read_text(encoding="utf-8")
    # Liens relatifs : le dossier se déplace avec le document.
    assert "](eco2mix_rapport_2026-06_graphiques/courbe_carbone.png)" in texte
    assert all((dossier / image).read_bytes().startswith(b"\x89PNG") for image in images)


def test_markdown_without_charts_is_text_only(medallion_db: Path, tmp_path: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(
        JUIN, settings=settings, format="md", graphiques=False, now=NOW
    )

    assert "![" not in chemin.read_text(encoding="utf-8")
    assert not (tmp_path / "eco2mix_rapport_2026-06_graphiques").exists()


def test_markdown_for_a_month_without_data(medallion_db: Path, tmp_path: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db, reports_dir=tmp_path)

    chemin = generate_monthly_report(date(2024, 1, 1), settings=settings, format="md", now=NOW)

    assert "Aucune donnée éCO2mix disponible" in chemin.read_text(encoding="utf-8")


def test_unknown_format_is_rejected(medallion_db: Path) -> None:
    settings = Settings(_env_file=None, duckdb_path=medallion_db)
    with pytest.raises(ValueError, match="format inconnu"):
        generate_monthly_report(JUIN, settings=settings, format="docx")  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["2026-13", "juin", "2026/06"])
def test_invalid_month_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="AAAA-MM"):
        parse_month(value)
