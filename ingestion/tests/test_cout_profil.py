"""Coût horaire d'un profil type : grille HP/HC, carbone et gain de décalage.

Le profil `bureau_36kva` (seed) appelle 28 kW de 8 h à 12 h les jours ouvrés,
dont 6 kW flexibles, et 6 kW la nuit et le week-end. Les données horaires sont
celles de `medallion.py` : le 30 juin 2026 (mardi, grille de février 2026) et
le 26 octobre 2025 (dimanche de 25 h, avant toute grille HP/HC).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingestion.diagnostic import CoutProfil, formater_diagnostic, lire_diagnostic
from ingestion.tests.medallion import DBT_BIN, JOUR_25H
from ingestion.tests.test_diagnostic import territoire

pytestmark = pytest.mark.skipif(DBT_BIN is None, reason="dbt non installé (uv sync --group dbt)")

#: Grille du 1er février 2026, € HTVA = prix HT + accise de 30,85 €/MWh.
PRIX_HP = 0.1351 + 0.03085
PRIX_HC = 0.0989 + 0.03085


@pytest.fixture(scope="module")
def warehouse(medallion_db: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect(str(medallion_db), read_only=True)
    yield connection
    connection.close()


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[tuple[Any, ...]]:
    return connection.execute(query).fetchall()


def _heure(warehouse: duckdb.DuckDBPyConnection, heure_utc: str) -> tuple[Any, ...]:
    return _rows(
        warehouse,
        "SELECT type_jour, periode_tarifaire, conso_kwh, conso_flexible_kwh,"
        " prix_htva_eur_kwh, cout_htva_eur, emissions_kgco2"
        f" FROM gold.fct_cout_horaire_profil WHERE heure_utc = '{heure_utc}'",
    )[0]


def test_weekday_morning_is_priced_at_peak_rate(warehouse: duckdb.DuckDBPyConnection) -> None:
    # 30 juin 2026, 10 h à Paris : 28 kW, dont 6 flexibles, en heures pleines.
    type_jour, periode, kwh, flexible, prix, cout, emissions = _heure(
        warehouse, "2026-06-30 08:00:00+00"
    )
    assert (type_jour, periode, kwh, flexible) == ("ouvre", "hp", 28.0, 6.0)
    assert prix == pytest.approx(PRIX_HP)
    assert cout == pytest.approx(28 * PRIX_HP)
    assert emissions == pytest.approx(28 * 20 / 1000)


def test_weekday_night_is_priced_at_off_peak_rate(warehouse: duckdb.DuckDBPyConnection) -> None:
    # 30 juin 2026, 23 h à Paris.
    assert _heure(warehouse, "2026-06-30 21:00:00+00")[1:5] == (
        "hc",
        6.0,
        0.0,
        pytest.approx(PRIX_HC),
    )


def test_sunday_of_25_hours_counts_every_hour_without_price(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    assert _rows(
        warehouse,
        "SELECT count(*), sum(conso_kwh), count(prix_htva_eur_kwh), min(type_jour)"
        f" FROM gold.fct_cout_horaire_profil WHERE jour = '{JOUR_25H}'",
    ) == [(25, 150.0, 0, "week_end")]


def test_month_outside_tariff_grid_has_no_cost(warehouse: duckdb.DuckDBPyConnection) -> None:
    assert _rows(
        warehouse,
        "SELECT cout_htva_eur, gain_decalage_eur, est_complet"
        " FROM gold.fct_cout_mensuel_profil WHERE mois = '2025-10-01'",
    ) == [(None, None, False)]


def test_shifting_flexible_load_saves_the_peak_off_peak_gap(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    # Juin 2026 : seul le 30 est mesuré. 6 kW flexibles x 4 h = 24 kWh reportés.
    kwh, gain, co2, complet = _rows(
        warehouse,
        "SELECT kwh_decales, gain_decalage_eur, co2_evite_kg, est_complet"
        " FROM gold.fct_cout_mensuel_profil WHERE mois = '2026-06-01'",
    )[0]
    assert kwh == 24.0
    assert gain == pytest.approx(24 * (PRIX_HP - PRIX_HC))
    # 20 gCO2/kWh le matin comme la nuit : rien d'évité.
    assert co2 == pytest.approx(0.0)
    assert complet is False


def test_incomplete_months_are_not_shown(medallion_db: Path) -> None:
    diagnostic = lire_diagnostic(territoire("69123"), database=medallion_db)
    assert diagnostic.cout is None
    assert "Aucun mois complet et tarifé" in formater_diagnostic(diagnostic)


COUT = CoutProfil(
    mois=date(2026, 9, 1),
    libelle="Bureau tertiaire, 36 kVA",
    conso_kwh=9_600.0,
    part_hc=0.15,
    cout_htva_eur=1_586.0,
    prix_moyen_htva_eur_kwh=0.1652,
    intensite_moyenne_g_kwh=22.2,
    kwh_decales=528.0,
    gain_decalage_eur=21.54,
    co2_evite_kg=-1.4,
)


def test_cost_section_reports_price_and_carbon_backfire(medallion_db: Path) -> None:
    diagnostic = dataclasses.replace(
        lire_diagnostic(territoire("69123"), database=medallion_db), cout=COUT
    )
    texte = formater_diagnostic(diagnostic)
    assert "septembre 2026" in texte
    assert "1 586 € HTVA, soit 0,1652 €/kWh" in texte
    assert "22 € économisés, mais 1,4 kgCO₂ émis en plus" in texte
    assert "identique sur tout le territoire" in texte


def test_cost_section_warns_about_zni_tariffs(medallion_db: Path) -> None:
    diagnostic = dataclasses.replace(
        lire_diagnostic(territoire("2A004"), database=medallion_db),
        cout=dataclasses.replace(COUT, co2_evite_kg=3.0),
    )
    texte = formater_diagnostic(diagnostic)
    assert "3,0 kgCO₂ évités" in texte
    assert "la Corse et l'outre-mer ont leurs propres barèmes" in texte
