"""Couches silver et gold : `dbt build` de bout en bout sur une base DuckDB de test.

Les données synthétiques et leurs pièges sont décrits dans `medallion.py`.
Suite ignorée si dbt n'est pas installé (`uv sync --group dbt`).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingestion.config import Settings
from ingestion.datasets import NATIONAL_TR
from ingestion.pipeline import ensure_bronze_tables
from ingestion.tests.medallion import (
    DBT_BIN,
    JOUR_25H,
    PRODUCTION_BAS_CARBONE,
    PRODUCTION_TOTALE,
    dbt_build,
    load_bronze,
    temps_reel,
)

pytestmark = pytest.mark.skipif(DBT_BIN is None, reason="dbt non installé (uv sync --group dbt)")


@pytest.fixture(scope="module")
def warehouse(medallion_db: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect(str(medallion_db), read_only=True)
    yield connection
    connection.close()


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[tuple[Any, ...]]:
    return connection.execute(query).fetchall()


def test_schemas_follow_medallion_layers(warehouse: duckdb.DuckDBPyConnection) -> None:
    tables = set(_rows(warehouse, "SELECT table_schema, table_name FROM information_schema.tables"))
    assert {
        ("bronze", "national_tr"),
        ("bronze", "national_cons_def"),
        ("silver", "national_tr"),
        ("silver", "national_cons_def"),
        ("silver", "mix_unifie"),
        ("gold", "dim_filiere"),
        ("gold", "fct_production_filiere"),
        ("gold", "fct_mix"),
        ("gold", "fct_mix_journalier"),
        ("bronze", "rte_tempo"),
        ("silver", "tempo_jours"),
        ("gold", "tarifs_tempo"),
        ("gold", "fct_creneau_horaire"),
    } <= tables


def test_silver_drops_rows_without_measures(warehouse: duckdb.DuckDBPyConnection) -> None:
    # Consolidé : 100 + 96 quarts d'heure en bronze, mais seules les demi-heures sont mesurées.
    bronze, silver = _rows(
        warehouse,
        "SELECT (SELECT count(*) FROM bronze.national_cons_def),"
        " (SELECT count(*) FROM silver.national_cons_def)",
    )[0]
    assert (bronze, silver) == (196, 98)
    # Temps réel : la ligne « à blanc » est écartée.
    assert _rows(warehouse, "SELECT count(*) FROM silver.national_tr") == [(23,)]


def test_real_time_only_takes_over_after_last_consolidated_measure(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    # Dernière demi-heure consolidée : 21:30 UTC, qui couvre jusqu'à 22:00.
    rows = _rows(
        warehouse,
        "SELECT min(date_heure), count(*) FROM silver.mix_unifie WHERE qualite = 'temps_reel'",
    )
    assert rows == [(datetime(2026, 6, 30, 22, 0, tzinfo=UTC), 15)]
    assert _rows(
        warehouse,
        "SELECT count(*) FROM silver.mix_unifie"
        " WHERE qualite = 'temps_reel' AND date_heure < '2026-06-30 22:00:00+00'",
    ) == [(0,)]


def test_daily_energy_accounts_for_25_hour_day(warehouse: duckdb.DuckDBPyConnection) -> None:
    row = _rows(
        warehouse,
        "SELECT qualite, nb_mesures, heures_du_jour, est_complet, consommation_gwh,"
        " production_gwh, part_bas_carbone, emissions_tco2"
        f" FROM gold.fct_mix_journalier WHERE jour = '{JOUR_25H}'",
    )[0]
    qualite, nb_mesures, heures, complet, consommation, production, part, emissions = row
    assert (qualite, nb_mesures, heures, complet) == ("definitive", 50, 25, True)
    assert consommation == pytest.approx(50_000 * 25 / 1000)
    assert production == pytest.approx(PRODUCTION_TOTALE * 25 / 1000)
    assert part == pytest.approx(PRODUCTION_BAS_CARBONE / PRODUCTION_TOTALE)
    # 20 gCO2/kWh x 1 262 500 MWh = 25 250 tCO2.
    assert emissions == pytest.approx(20 * PRODUCTION_TOTALE * 25 / 1000)


def test_partial_day_is_flagged(warehouse: duckdb.DuckDBPyConnection) -> None:
    # Le 1er juillet n'est couvert par le temps réel que de 00:00 à 03:45 (heure de Paris).
    assert _rows(
        warehouse,
        "SELECT qualite, est_complet FROM gold.fct_mix_journalier WHERE jour = '2026-07-01'",
    ) == [("temps_reel", False)]


def test_production_by_sector_covers_every_sector(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = _rows(
        warehouse,
        "SELECT famille, sum(production_mw) FROM gold.fct_production_filiere"
        " WHERE date_heure = '2025-10-26 12:00:00+00' GROUP BY famille ORDER BY famille",
    )
    assert rows == [("fossile", 1_000.0), ("nucleaire", 40_000.0), ("renouvelable", 9_500.0)]


def test_first_hourly_run_builds_without_consolidated_data(tmp_path: Path) -> None:
    """Avant tout run mensuel, le consolidé est vide : le temps réel couvre seul la série."""
    database = tmp_path / "eco2mix.duckdb"
    load_bronze(database, tmp_path, NATIONAL_TR, temps_reel())
    ensure_bronze_tables(settings=Settings(_env_file=None, duckdb_path=database))

    dbt_build(database, tmp_path)

    with duckdb.connect(str(database), read_only=True) as connection:
        assert _rows(connection, "SELECT qualite, count(*) FROM gold.fct_mix GROUP BY qualite") == [
            ("temps_reel", 23)
        ]


# --- Tempo et prix -------------------------------------------------------------


def _creneau(warehouse: duckdb.DuckDBPyConnection, heure_utc: str) -> tuple[Any, ...]:
    return _rows(
        warehouse,
        "SELECT jour_tempo, couleur_tempo, periode_tarifaire, prix_ttc_eur_kwh"
        f" FROM gold.fct_creneau_horaire WHERE heure_utc = '{heure_utc}'",
    )[0]


def test_early_morning_hours_belong_to_previous_tempo_day(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    # 30 juin, 5 h à Paris : encore le jour Tempo du 29 (bleu), en heures creuses.
    jour, couleur, periode, prix = _creneau(warehouse, "2026-06-30 03:00:00+00")
    assert (str(jour), couleur, periode) == ("2026-06-29", "bleu", "hc")
    assert prix == pytest.approx(0.1325)


def test_red_day_peak_hours_use_red_peak_price(warehouse: duckdb.DuckDBPyConnection) -> None:
    # 30 juin, 12 h à Paris : jour rouge, heures pleines.
    assert _creneau(warehouse, "2026-06-30 10:00:00+00")[1:3] == ("rouge", "hp")
    assert _creneau(warehouse, "2026-06-30 10:00:00+00")[3] == pytest.approx(0.7060)
    # 30 juin, 23 h à Paris : toujours le jour rouge, mais en heures creuses.
    assert _creneau(warehouse, "2026-06-30 21:00:00+00")[1:3] == ("rouge", "hc")


def test_hours_outside_tariff_grid_have_no_price(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = _rows(
        warehouse,
        "SELECT count(*), count(couleur_tempo), count(prix_ttc_eur_kwh)"
        f" FROM gold.fct_creneau_horaire WHERE jour = '{JOUR_25H}'",
    )
    # 25 heures le jour du changement d'heure, couleur connue, aucune grille avant 2026.
    assert rows == [(25, 25, 0)]


def test_hourly_carbon_intensity_is_weighted_by_production(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    # 30 juin, 19 h à Paris : les deux demi-heures sont en pic.
    assert _rows(
        warehouse,
        "SELECT taux_co2_g_kwh, nb_mesures, est_complete FROM gold.fct_creneau_horaire"
        " WHERE heure_utc = '2026-06-30 17:00:00+00'",
    ) == [(80.0, 2, True)]
