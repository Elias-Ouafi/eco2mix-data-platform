"""Clé de MERGE composite et datasets territoriaux.

Tous les datasets ne sont pas des séries temporelles : un équilibre régional est
identifié par `(mois, code_insee_region)`, un relevé de contraintes par la seule
`region`. Ces tests vérifient que l'idempotence, les contrôles et l'extraction
tiennent sur ces formes-là. Aucun appel réseau réel.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from ingestion.config import Settings
from ingestion.datasets import (
    CONSO_IRIS,
    CONTRAINTES_REGIONALES,
    EQUILIBRE_REGIONAL,
    NATIONAL_TR,
    REGIONAL_CONS_DEF,
    ColumnKind,
    DatasetSpec,
    code,
    instant,
    label,
    measures,
)
from ingestion.extract_odre import ExtractionWindow
from ingestion.pipeline import extract_to_parquet
from ingestion.transform import records_to_table, write_parquet
from ingestion.warehouse import DuckDBLoader

INGESTED_AT = datetime(2026, 10, 9, 6, 0, tzinfo=UTC)


def build_parquet(
    records: list[dict[str, Any]],
    *,
    spec: DatasetSpec,
    tmp_path: Path,
    run_id: str,
    ingested_at: datetime = INGESTED_AT,
) -> Path:
    table = records_to_table(records, spec=spec, ingested_at=ingested_at)
    return write_parquet(
        table,
        dataset_dir=tmp_path / spec.slug,
        run_id=run_id,
        ingest_date=date(2026, 10, 9),
    )


# --- Déclaration des specs -------------------------------------------------


def test_a_spec_must_declare_a_key() -> None:
    with pytest.raises(ValueError, match="au moins une colonne"):
        DatasetSpec(dataset_id="x", keys=(), attributes=(), measures=(), description="sans cle")


def test_a_spec_accepts_only_one_instant() -> None:
    """Deux instants dans la clé rendraient la normalisation du fuseau ambiguë."""
    with pytest.raises(ValueError, match="une seule colonne d'instant"):
        DatasetSpec(
            dataset_id="x",
            keys=(instant("debut"), instant("fin")),
            attributes=(),
            measures=(),
            description="deux instants",
        )


def test_a_spec_refuses_duplicate_columns() -> None:
    with pytest.raises(ValueError, match="colonnes en double"):
        DatasetSpec(
            dataset_id="x",
            keys=(code("region"),),
            attributes=(label("region"),),
            measures=(),
            description="doublon",
        )


def test_time_series_detection() -> None:
    assert NATIONAL_TR.is_time_series
    assert NATIONAL_TR.instant_column == "date_heure"
    assert REGIONAL_CONS_DEF.is_time_series
    assert REGIONAL_CONS_DEF.key_names == ("date_heure", "code_insee_region")

    assert not CONTRAINTES_REGIONALES.is_time_series
    assert CONTRAINTES_REGIONALES.instant_column is None
    assert CONTRAINTES_REGIONALES.key_names == ("region",)


def test_key_columns_are_not_nullable() -> None:
    """L'unicité du MERGE repose sur des clés toujours renseignées."""
    schema = EQUILIBRE_REGIONAL.schema
    for name in EQUILIBRE_REGIONAL.key_names:
        assert not schema.field(name).nullable, name
    assert schema.field("production_totale").nullable


def test_regional_dataset_has_no_carbon_intensity() -> None:
    """RTE ne publie pas de `taux_co2` régional : le spec ne doit pas en inventer un."""
    assert "taux_co2" in NATIONAL_TR.schema.names
    assert "taux_co2" not in REGIONAL_CONS_DEF.schema.names


# --- Normalisation du fuseau, par groupe -----------------------------------


def test_october_ambiguity_is_resolved_per_region() -> None:
    """Le même instant local est publié pour chaque région : le groupe tranche.

    Sans groupement, la deuxième région d'un instant répété serait vue comme un
    retour en arrière et basculerait à tort en heure d'hiver.
    """
    records = [
        {"date_heure": "2026-10-25 02:00:00", "code_insee_region": "11"},
        {"date_heure": "2026-10-25 02:00:00", "code_insee_region": "84"},
        {"date_heure": "2026-10-25 02:00:00", "code_insee_region": "11"},
        {"date_heure": "2026-10-25 02:00:00", "code_insee_region": "84"},
    ]

    table = records_to_table(records, spec=REGIONAL_CONS_DEF, ingested_at=INGESTED_AT)

    par_region: dict[str, list[datetime]] = {}
    for row in table.select(["date_heure", "code_insee_region"]).to_pylist():
        par_region.setdefault(row["code_insee_region"], []).append(row["date_heure"])

    # Chaque région voit l'heure d'été (00:00 UTC) puis l'heure d'hiver (01:00 UTC).
    for region, instants in par_region.items():
        assert instants == [
            datetime(2026, 10, 25, 0, 0, tzinfo=UTC),
            datetime(2026, 10, 25, 1, 0, tzinfo=UTC),
        ], region


def test_records_missing_part_of_the_key_are_dropped() -> None:
    records = [
        {"mois": "2026-06", "code_insee_region": "11", "consommation_brute": 1.0},
        {"mois": "2026-06", "consommation_brute": 2.0},  # région manquante
        {"code_insee_region": "11", "consommation_brute": 3.0},  # mois manquant
    ]

    table = records_to_table(records, spec=EQUILIBRE_REGIONAL, ingested_at=INGESTED_AT)

    assert table.num_rows == 1


def test_period_keys_are_kept_as_published() -> None:
    """La couche bronze reste le miroir de la source : `mois` n'est pas casté."""
    table = records_to_table(
        [{"mois": "2014-06", "code_insee_region": "84"}],
        spec=EQUILIBRE_REGIONAL,
        ingested_at=INGESTED_AT,
    )

    assert table.column("mois").to_pylist() == ["2014-06"]
    assert EQUILIBRE_REGIONAL.keys[0].kind is ColumnKind.PERIOD


# --- MERGE sur clé composite -----------------------------------------------


def test_composite_merge_keeps_one_row_per_region_and_instant(tmp_path: Path) -> None:
    records = [
        {
            "date_heure": "2026-06-01T00:00:00+00:00",
            "code_insee_region": "11",
            "consommation": 5000,
        },
        {
            "date_heure": "2026-06-01T00:00:00+00:00",
            "code_insee_region": "84",
            "consommation": 4000,
        },
        {
            "date_heure": "2026-06-01T00:15:00+00:00",
            "code_insee_region": "11",
            "consommation": 5100,
        },
    ]
    parquet = build_parquet(records, spec=REGIONAL_CONS_DEF, tmp_path=tmp_path, run_id="r1")

    with DuckDBLoader(":memory:", spec=REGIONAL_CONS_DEF) as loader:
        first = loader.merge_parquet(parquet)
        replay = loader.merge_parquet(parquet)

        assert first.rows_inserted == 3
        assert replay.rows_inserted == 0
        assert replay.rows_updated == 3
        assert loader.row_count() == 3


def test_composite_merge_revises_only_the_matching_key(tmp_path: Path) -> None:
    """Une révision sur une région ne doit pas toucher les autres."""
    initial = build_parquet(
        [
            {
                "date_heure": "2026-06-01T00:00:00+00:00",
                "code_insee_region": "11",
                "consommation": 5000,
            },
            {
                "date_heure": "2026-06-01T00:00:00+00:00",
                "code_insee_region": "84",
                "consommation": 4000,
            },
        ],
        spec=REGIONAL_CONS_DEF,
        tmp_path=tmp_path,
        run_id="r1",
    )
    revision = build_parquet(
        [
            {
                "date_heure": "2026-06-01T00:00:00+00:00",
                "code_insee_region": "84",
                "consommation": 4200,
            }
        ],
        spec=REGIONAL_CONS_DEF,
        tmp_path=tmp_path,
        run_id="r2",
        ingested_at=datetime(2026, 10, 9, 7, 0, tzinfo=UTC),
    )

    with DuckDBLoader(":memory:", spec=REGIONAL_CONS_DEF) as loader:
        loader.merge_parquet(initial)
        result = loader.merge_parquet(revision)

        assert result.rows_updated == 1
        assert loader.row_count() == 2
        rows = dict(
            loader.connection.execute(
                "SELECT code_insee_region, consommation FROM bronze.regional_cons_def"
            ).fetchall()
        )
        assert rows == {"11": 5000.0, "84": 4200.0}


def test_primary_key_covers_the_whole_tuple(tmp_path: Path) -> None:
    """La clé primaire composite rend l'unicité structurelle, pas seulement procédurale."""
    with DuckDBLoader(":memory:", spec=EQUILIBRE_REGIONAL) as loader:
        loader.ensure_table()
        keys = loader.connection.execute(
            "SELECT constraint_text FROM duckdb_constraints() "
            "WHERE table_name = 'equilibre_regional_mensuel_prod_conso_brute' "
            "AND constraint_type = 'PRIMARY KEY'"
        ).fetchone()

        assert keys is not None
        assert "mois" in keys[0] and "code_insee_region" in keys[0]


@pytest.mark.parametrize("ordre", [("pleine", "vide"), ("vide", "pleine")])
def test_duplicate_key_keeps_the_most_complete_row(tmp_path: Path, ordre: tuple[str, str]) -> None:
    """ODRÉ publie parfois un doublon dont toutes les mesures sont nulles.

    Les deux lignes partageant le même `ingested_at_utc`, un départage sur ce seul
    horodatage serait arbitraire et perdrait la ligne renseignée une fois sur deux
    (constaté : 118 clés sur 233 dans `consommation-annuelle-par-iris`).
    Le résultat ne doit pas dépendre de l'ordre d'arrivée.
    """
    lignes = {
        "pleine": {
            "annee": "2023",
            "code_iris": "132110802",
            "commune": "Marseille",
            "consommation_electricite_rte": 13503.25,
            "pdl_electricite_rte": 1,
        },
        "vide": {"annee": "2023", "code_iris": "132110802", "commune": "Marseille"},
    }
    parquet = build_parquet(
        [lignes[nom] for nom in ordre],
        spec=CONSO_IRIS,
        tmp_path=tmp_path,
        run_id=f"dup-{'-'.join(ordre)}",
    )

    with DuckDBLoader(":memory:", spec=CONSO_IRIS) as loader:
        result = loader.merge_parquet(parquet)

        assert result.rows_in_file == 2
        assert result.rows_merged == 1
        assert loader.connection.execute(
            "SELECT consommation_electricite_rte FROM bronze.consommation_annuelle_par_iris"
        ).fetchone() == (13503.25,)


# --- Dataset sans dimension temporelle -------------------------------------


def test_merge_works_without_any_instant(tmp_path: Path) -> None:
    parquet = build_parquet(
        [
            {"region": "GRAND EST", "puissance_enr_installee": 10679.0},
            {"region": "BRETAGNE", "puissance_enr_installee": 2500.0},
        ],
        spec=CONTRAINTES_REGIONALES,
        tmp_path=tmp_path,
        run_id="r1",
    )

    with DuckDBLoader(":memory:", spec=CONTRAINTES_REGIONALES) as loader:
        loader.merge_parquet(parquet)
        replay = loader.merge_parquet(parquet)

        assert loader.row_count() == 2
        assert replay.rows_updated == 2


def test_temporal_checks_fail_clearly_on_a_territorial_dataset() -> None:
    """Mieux vaut une erreur explicite qu'un contrôle de fraîcheur qui ne veut rien dire."""
    with DuckDBLoader(":memory:", spec=CONTRAINTES_REGIONALES) as loader:
        loader.ensure_table()

        with pytest.raises(ValueError, match="pas de controle temporel"):
            loader.latest_timestamp()


# --- Extraction intégrale --------------------------------------------------


def test_full_extraction_sends_no_window_and_sorts_on_the_key(
    httpx_mock: HTTPXMock,
    settings: Settings,
) -> None:
    """Un `where` sur `date_heure` ferait une 400 sur un dataset qui n'a pas ce champ."""
    httpx_mock.add_response(
        url=(
            f"{settings.api_base_url}/catalog/datasets/"
            "energies-et-puissances-regionales-liees-au-contraintes/exports/csv"
            "?order_by=region&timezone=UTC&delimiter=%3B&use_labels=false"
        ),
        text="region;puissance_enr_installee\nGRAND EST;10679.0\n",
    )

    outcome = extract_to_parquet(
        settings=settings, spec=CONTRAINTES_REGIONALES, run_id="t1", now=INGESTED_AT
    )

    assert outcome.rows == 1
    assert outcome.api_calls == 1
    assert outcome.window_start is None
    assert Path(outcome.parquet_path).exists()


def test_a_window_is_refused_on_a_territorial_dataset(settings: Settings) -> None:
    with pytest.raises(ValueError, match="pas de dimension temporelle"):
        extract_to_parquet(
            settings=settings,
            spec=CONTRAINTES_REGIONALES,
            window=ExtractionWindow.last_hours(3, now=INGESTED_AT),
        )


def test_territorial_specs_declare_only_known_measures() -> None:
    """Garde-fou contre une faute de frappe : toute mesure doit être un float64."""
    for spec in (CONTRAINTES_REGIONALES, EQUILIBRE_REGIONAL):
        for column in spec.measures:
            assert column.kind is ColumnKind.MEASURE
            assert spec.schema.field(column.name).type == measures("x")[0].arrow_type
