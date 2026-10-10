"""Diagnostic d'implantation : couches silver et gold territoriales, puis lecture et CLI.

Les données synthétiques et leurs pièges sont décrits dans `medallion.py`. La
base est construite une fois par session par `dbt build` ; aucun appel réseau
réel (la BAN est simulée par `pytest-httpx`).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_httpx import HTTPXMock

from ingestion import cli
from ingestion.config import Settings
from ingestion.diagnostic import (
    DiagnosticIndisponibleError,
    code_commune_parent,
    code_departement,
    formater_diagnostic,
    lire_diagnostic,
)
from ingestion.geocode import Territoire
from ingestion.tests.medallion import ANNEE_REFERENCE, DBT_BIN

pytestmark = pytest.mark.skipif(DBT_BIN is None, reason="dbt non installé (uv sync --group dbt)")


@pytest.fixture(scope="module")
def warehouse(medallion_db: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect(str(medallion_db), read_only=True)
    yield connection
    connection.close()


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[tuple[Any, ...]]:
    return connection.execute(query).fetchall()


def territoire(code_commune: str, commune: str = "Commune", **overrides: Any) -> Territoire:
    valeurs: dict[str, Any] = {
        "adresse": f"1 rue de test, {commune}",
        "code_insee_commune": code_commune,
        "commune": commune,
        "code_postal": None,
        "code_departement": code_departement(code_commune),
        "departement": "",
        "code_insee_region": None,
        "region": "",
        "latitude": 0.0,
        "longitude": 0.0,
        "score": 0.9,
        "precision": "housenumber",
    }
    return Territoire(**(valeurs | overrides))


# --- Hiérarchie territoriale (Python) ----------------------------------------


@pytest.mark.parametrize(
    ("code", "parent"),
    [("69383", "69123"), ("75101", "75056"), ("13216", "13055"), ("69266", "69266")],
)
def test_arrondissements_are_brought_back_to_their_commune(code: str, parent: str) -> None:
    assert code_commune_parent(code) == parent


@pytest.mark.parametrize(
    ("commune", "departement"), [("69123", "69"), ("2A004", "2A"), ("97411", "974")]
)
def test_department_is_derived_from_commune_code(commune: str, departement: str) -> None:
    assert code_departement(commune) == departement


# --- Silver ------------------------------------------------------------------


def test_iris_codes_are_padded_and_empty_duplicates_dropped(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(
        warehouse,
        "SELECT code_iris, consommation_electricite_mwh FROM silver.conso_industrielle_iris"
        " WHERE annee = 2022 ORDER BY code_iris",
    )
    # « 13760000 » vide doublonnait « 013760000 » : la ligne publiée sur 9 caractères reste.
    assert rows == [("013760000", 7_000.0), ("692660000", 99_000.0), ("693830101", None)]


def test_missing_commune_is_rebuilt_from_iris_code(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = _rows(
        warehouse,
        "SELECT code_iris, code_insee_commune, code_departement, code_insee_region"
        " FROM silver.conso_industrielle_iris"
        " WHERE est_commune_reconstituee AND annee = 2021 ORDER BY code_iris",
    )
    assert rows == [
        ("020030000", "02003", "02", "32"),  # zéro de tête restauré
        ("751010101", "75056", "75", "11"),  # arrondissement ramené à Paris
    ]


def test_corsica_resolves_its_hierarchy(warehouse: duckdb.DuckDBPyConnection) -> None:
    assert _rows(
        warehouse,
        "SELECT code_departement, code_insee_region FROM silver.conso_industrielle_iris"
        " WHERE code_iris = '2A0040000'",
    ) == [("2A", "94")]


def test_constraints_region_label_is_matched_to_its_code(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    assert _rows(
        warehouse,
        "SELECT code_insee_region, libelle_publie FROM silver.contraintes_reseau_region"
        " ORDER BY code_insee_region",
    ) == [("11", "ÎLE-DE-FRANCE"), ("84", "AUVERGNE-RHÔNE-ALPES")]


# --- Gold : pression industrielle ---------------------------------------------


def test_reference_year_is_last_one_without_statistical_secrecy(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    assert _rows(
        warehouse, "SELECT DISTINCT annee FROM gold.fct_pression_industrielle_departement"
    ) == [(ANNEE_REFERENCE,)]
    assert _rows(
        warehouse, "SELECT DISTINCT annee FROM gold.fct_pression_industrielle_commune"
    ) == [(ANNEE_REFERENCE,)]


def test_department_pressure_aggregates_communes_and_ranks(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(
        warehouse,
        "SELECT code_departement, consommation_electricite_mwh, nb_sites_electricite,"
        " nb_communes_avec_site, rang_national, nb_departements_classes"
        " FROM gold.fct_pression_industrielle_departement"
        " WHERE rang_national IS NOT NULL ORDER BY rang_national",
    )
    assert rows == [
        ("69", 100_000.0, 3, 2, 1, 4),
        ("02", 10_000.0, 1, 1, 2, 4),  # IRIS sans commune publiée : compté quand même
        ("75", 5_000.0, 1, 1, 3, 4),
        ("2A", 3_000.0, 1, 1, 4, 4),
    ]


def test_every_department_has_a_row_even_without_site(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    assert _rows(
        warehouse,
        "SELECT count(*), count(*) FILTER (WHERE conso_industrielle_departement_gwh = 0)"
        " FROM gold.fct_diagnostic_territoire",
    ) == [(101, 97)]


# --- Gold : tension du réseau régional ------------------------------------------


def test_regional_balance_uses_last_twelve_months(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = _rows(
        warehouse,
        "SELECT code_insee_region, mois_debut, mois_fin, nb_mois, production_gwh,"
        " taux_couverture, statut_equilibre, rang_dependance"
        " FROM gold.fct_tension_reseau_region WHERE nb_mois > 0 ORDER BY rang_dependance",
    )
    debut, fin = date(2025, 1, 1), date(2025, 12, 1)
    assert rows == [
        ("11", debut, fin, 12, pytest.approx(0.12), pytest.approx(0.1), "deficitaire", 1),
        ("94", debut, fin, 12, pytest.approx(0.84), pytest.approx(0.7), "deficitaire", 2),
        ("84", debut, fin, 12, pytest.approx(2.4), pytest.approx(2.0), "excedentaire", 3),
    ]


def test_regions_without_data_are_kept_with_nulls(warehouse: duckdb.DuckDBPyConnection) -> None:
    assert _rows(
        warehouse,
        "SELECT nb_mois, taux_couverture, rang_dependance, nb_regions_classees"
        " FROM gold.fct_tension_reseau_region WHERE code_insee_region = '04'",
    ) == [(0, None, None, 3)]


def test_curtailed_energy_sums_the_four_seasons(warehouse: duckdb.DuckDBPyConnection) -> None:
    assert _rows(
        warehouse,
        "SELECT puissance_a_compenser_mw, energie_non_evacuee_mwh"
        " FROM gold.fct_tension_reseau_region WHERE code_insee_region = '84'",
    ) == [(187.0, 2_777.0)]


# --- Lecture du diagnostic ------------------------------------------------------


def test_diagnostic_for_a_lyon_arrondissement(medallion_db: Path) -> None:
    diagnostic = lire_diagnostic(territoire("69383", "Lyon"), puissance_mw=5, database=medallion_db)
    assert diagnostic.code_insee_commune == "69123"
    assert (diagnostic.code_departement, diagnostic.code_insee_region) == ("69", "84")
    assert diagnostic.commune_a_des_sites
    assert diagnostic.conso_industrielle_commune_gwh == pytest.approx(40.0)
    assert diagnostic.conso_industrielle_departement_gwh == pytest.approx(100.0)
    assert diagnostic.rang_pression_departement == 1
    assert diagnostic.statut_equilibre == "excedentaire"
    assert diagnostic.seuil is not None
    assert diagnostic.seuil.domaine_tension == "HTA"


def test_commune_without_site_falls_back_to_department(medallion_db: Path) -> None:
    diagnostic = lire_diagnostic(territoire("69029", "Bron"), database=medallion_db)
    assert not diagnostic.commune_a_des_sites
    assert diagnostic.nb_sites_departement == 3
    assert diagnostic.seuil is None
    assert "repli sur le département" in formater_diagnostic(diagnostic)


@pytest.mark.parametrize(
    ("puissance", "domaine"),
    [
        (0.009, "BT ≤ 36 kVA"),
        (0.036, "BT > 36 kVA"),
        (0.25, "HTA"),
        (19.9, "HTA"),
        (20, "HTA – zone grise"),
        (39.9, "HTA – zone grise"),
        (40, "HTB"),
        (1_000, "HTB"),
    ],
)
def test_connection_threshold_follows_project_power(
    medallion_db: Path, puissance: float, domaine: str
) -> None:
    seuil = lire_diagnostic(
        territoire("69123"), puissance_mw=puissance, database=medallion_db
    ).seuil
    assert seuil is not None
    assert seuil.domaine_tension == domaine


def test_every_figure_comes_with_its_limit(medallion_db: Path, warehouse: Any) -> None:
    texte = formater_diagnostic(
        lire_diagnostic(territoire("69123"), puissance_mw=50, database=medallion_db)
    )
    limites = [
        limite for (limite,) in _rows(warehouse, "SELECT limite FROM gold.limites_methodologiques")
    ]
    assert len(limites) == 6
    for limite in limites:
        assert limite in texte


def test_non_interconnected_zones_are_flagged(medallion_db: Path) -> None:
    corse = formater_diagnostic(lire_diagnostic(territoire("2A004"), database=medallion_db))
    assert "Zone non interconnectée" in corse
    reunion = formater_diagnostic(lire_diagnostic(territoire("97411"), database=medallion_db))
    assert "Aucun équilibre régional publié par RTE" in reunion
    assert "zone non interconnectée" in reunion


def test_missing_warehouse_is_reported(tmp_path: Path) -> None:
    with pytest.raises(DiagnosticIndisponibleError, match="introuvable"):
        lire_diagnostic(territoire("69123"), database=tmp_path / "absent.duckdb")


def test_warehouse_without_gold_layer_is_reported(tmp_path: Path) -> None:
    database = tmp_path / "vide.duckdb"
    duckdb.connect(str(database)).close()
    with pytest.raises(DiagnosticIndisponibleError, match="dbt build"):
        lire_diagnostic(territoire("69123"), database=database)


# --- Ligne de commande -----------------------------------------------------------


def test_diagnose_command_end_to_end(
    httpx_mock: HTTPXMock,
    settings: Settings,
    medallion_db: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    httpx_mock.add_response(
        json={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [4.85, 45.75]},
                    "properties": {
                        "label": "10 Rue Garibaldi 69003 Lyon",
                        "score": 0.95,
                        "type": "housenumber",
                        "citycode": "69383",
                        "city": "Lyon",
                        "postcode": "69003",
                        "context": "69, Rhône, Auvergne-Rhône-Alpes",
                    },
                }
            ],
        }
    )
    reglages = settings.model_copy(
        update={"ban_base_url": "https://ban.test/geocodage", "duckdb_path": medallion_db}
    )
    monkeypatch.setattr(cli, "get_settings", lambda: reglages)

    code = cli.main(["diagnose", "--adresse", "10 rue Garibaldi, Lyon", "--puissance", "0,5"])

    sortie = capsys.readouterr().out
    assert code == 0
    assert "Lyon (69123) · Rhône (69) · Auvergne-Rhône-Alpes (84)" in sortie
    assert "rang 1 sur 4 départements" in sortie
    assert "Puissance 0,5 MW → HTA" in sortie


def test_diagnose_rejects_negative_power() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["diagnose", "--adresse", "x", "--puissance", "-1"])
