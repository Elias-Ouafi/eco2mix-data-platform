"""Base DuckDB de test, de bronze à gold, partagée par les tests dbt et du rapport.

La couche bronze est construite par le vrai code d'ingestion (Parquet puis
MERGE), à partir d'enregistrements synthétiques qui reproduisent les pièges
observés sur les API :

* le consolidé publie une ligne par quart d'heure mais ne mesure qu'à la
  demi-heure ;
* le temps réel publie sa ligne la plus récente « à blanc » ;
* les deux datasets se recouvrent dans le temps ;
* un jour de changement d'heure dure 25 h ;
* un jour Tempo court de 6 h à 6 h, à cheval sur deux jours civils ;
* des codes IRIS publiés sur 8 caractères (zéro de tête perdu), sans commune, et
  parfois en doublon vide d'une ligne correctement codée ;
* Paris, Lyon et Marseille publiés sous le code de la commune, pas de
  l'arrondissement ;
* une année récente partiellement masquée par le secret statistique ;
* une région identifiée par son libellé en majuscules, et non par son code.

Silver et gold sont ensuite construites par `dbt build`, lancé en sous-processus
comme le fait le DAG : dbt-duckdb garde sa connexion ouverte dans le processus
appelant, ce qui empêcherait de relire la base.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ingestion.config import Settings
from ingestion.datasets import (
    CONSO_IRIS,
    CONTRAINTES_REGIONALES,
    EQUILIBRE_REGIONAL,
    NATIONAL_CONS_DEF,
    NATIONAL_TR,
    RTE_TEMPO,
    DatasetSpec,
)
from ingestion.pipeline import ensure_bronze_tables
from ingestion.transform import PARIS_TZ, records_to_table, write_parquet
from ingestion.warehouse import DuckDBLoader

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
DBT_BIN = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")

#: Puissances constantes (MW) : les énergies attendues se calculent de tête.
MESURES = {
    "consommation": 50_000.0,
    "nucleaire": 40_000.0,
    "eolien": 5_000.0,
    "solaire": 1_000.0,
    "hydraulique": 3_000.0,
    "gaz": 1_000.0,
    "fioul": 0.0,
    "charbon": 0.0,
    "bioenergies": 500.0,
    "pompage": -500.0,
    "ech_physiques": -500.0,
    "taux_co2": 20.0,
}
PRODUCTION_TOTALE = 50_500.0
PRODUCTION_BAS_CARBONE = 49_500.0

#: Jour de retour à l'heure d'hiver : 25 h en heure de Paris.
JOUR_25H = date(2025, 10, 26)
#: Dernier jour couvert par le consolidé, recouvert ensuite par le temps réel.
JOUR_BASCULE = date(2026, 6, 30)

#: Pic de carbone placé en soirée du jour de bascule (heures locales 19 h et 20 h).
PIC_CO2 = 80.0
HEURES_PIC = (19, 20)

#: Calendrier Tempo : la veille du jour de bascule est bleue, le jour lui-même rouge.
TEMPO = {
    date(2025, 10, 25): "BLUE",
    JOUR_25H: "BLUE",
    date(2026, 6, 29): "BLUE",
    JOUR_BASCULE: "RED",
    date(2026, 7, 1): "WHITE",
}


def quarts_d_heure(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current < end:
        yield current
        current += timedelta(minutes=15)


def jour_utc(jour: date) -> tuple[datetime, datetime]:
    start = datetime(jour.year, jour.month, jour.day, tzinfo=PARIS_TZ)
    lendemain = jour + timedelta(days=1)
    end = datetime(lendemain.year, lendemain.month, lendemain.day, tzinfo=PARIS_TZ)
    return start.astimezone(UTC), end.astimezone(UTC)


def _record(instant: datetime, nature: str, *, mesure: bool, **overrides: float) -> dict[str, Any]:
    record: dict[str, Any] = {
        "date_heure": instant.isoformat(),
        "perimetre": "France",
        "nature": nature,
    }
    if mesure:
        record |= MESURES | overrides
        if instant.astimezone(PARIS_TZ).date() == JOUR_BASCULE and (
            instant.astimezone(PARIS_TZ).hour in HEURES_PIC
        ):
            record["taux_co2"] = PIC_CO2
    return record


def consolide() -> list[dict[str, Any]]:
    """Mesures à :00 et :30 seulement, lignes vides à :15 et :45 (comme l'API)."""
    records = []
    for jour, nature in ((JOUR_25H, "Données définitives"), (JOUR_BASCULE, "Données consolidées")):
        for instant in quarts_d_heure(*jour_utc(jour)):
            records.append(_record(instant, nature, mesure=instant.minute in (0, 30)))
    return records


def temps_reel() -> list[dict[str, Any]]:
    """Recouvre la fin du consolidé, puis le prolonge ; dernière ligne « à blanc »."""
    start = datetime(2026, 6, 30, 20, 0, tzinfo=UTC)
    end = datetime(2026, 7, 1, 2, 0, tzinfo=UTC)
    instants = list(quarts_d_heure(start, end))
    # Une consommation distincte permet de reconnaître la source dans le résultat.
    return [
        _record(instant, "Données temps réel", mesure=instant != instants[-1], consommation=45_000)
        for instant in instants
    ]


def tempo() -> list[dict[str, Any]]:
    """Format renvoyé par `RteTempoClient.fetch_calendar` : minuit de Paris + couleur."""
    return [
        {"date_heure": datetime(j.year, j.month, j.day, tzinfo=PARIS_TZ).isoformat(), "couleur": c}
        for j, c in sorted(TEMPO.items())
    ]


# --- Jeux territoriaux ---------------------------------------------------------

#: Année complète, retenue comme millésime de référence : 2022 a du secret statistique.
ANNEE_REFERENCE = 2021


def _iris(
    annee: int,
    code_iris: str,
    conso: float | None,
    sites: float | None,
    *,
    commune: tuple[str, str, str, str] | None = None,
) -> dict[str, Any]:
    """`commune` = (code commune, nom, code département, code région), ou rien."""
    record: dict[str, Any] = {
        "annee": str(annee),
        "code_iris": code_iris,
        "consommation_electricite_rte": conso,
        "pdl_electricite_rte": sites,
    }
    if commune is not None:
        code_commune, nom, departement, region = commune
        record |= {
            "code_insee_commune": code_commune,
            "commune": nom,
            "code_insee_departement": departement,
            "code_insee_region": region,
        }
    return record


LYON = ("69123", "Lyon", "69", "84")
VILLEURBANNE = ("69266", "Villeurbanne", "69", "84")
AJACCIO = ("2A004", "Ajaccio", "2A", "94")
SAINT_MAURICE = ("01376", "Saint-Maurice-de-Beynost", "01", "84")


def conso_iris() -> list[dict[str, Any]]:
    """MWh par an. Département 69 en 2021 : 40 000 + 60 000 MWh, 3 sites, 2 communes."""
    return [
        # 2021, millésime complet.
        _iris(2021, "693830101", 40_000.0, 1.0, commune=LYON),  # IRIS d'arrondissement
        _iris(2021, "692660000", 60_000.0, 2.0, commune=VILLEURBANNE),
        _iris(2021, "20030000", 10_000.0, 1.0),  # 020030000, Aisne, commune non publiée
        _iris(2021, "751010101", 5_000.0, 1.0),  # Paris 1er, commune non publiée
        _iris(2021, "2A0040000", 3_000.0, 1.0, commune=AJACCIO),
        # 2022, plus récent mais partiellement masqué.
        _iris(2022, "693830101", None, None, commune=LYON),
        _iris(2022, "692660000", 99_000.0, 2.0, commune=VILLEURBANNE),
        _iris(2022, "013760000", 7_000.0, 1.0, commune=SAINT_MAURICE),
        _iris(2022, "13760000", None, None),  # doublon vide du précédent
    ]


def equilibre_regional() -> list[dict[str, Any]]:
    """13 mois, de décembre 2024 à décembre 2025 : la fenêtre n'en retient que 12.

    Décembre 2024 porte une production aberrante, qui fausserait tout s'il était compté.
    """
    regions = {"84": 200.0, "11": 10.0, "94": 70.0}  # production mensuelle, conso = 100
    mois = ["2024-12"] + [f"2025-{m:02d}" for m in range(1, 13)]
    return [
        {
            "mois": m,
            "code_insee_region": code_region,
            "production_totale": 1e9 if m == "2024-12" else production,
            "consommation_brute": 100.0,
        }
        for m in mois
        for code_region, production in regions.items()
    ]


def contraintes_regionales() -> list[dict[str, Any]]:
    return [
        {
            "region": "AUVERGNE-RHÔNE-ALPES",
            "puissance_enr_installee": 14_575.0,
            "puissance_totale_a_compenser": 187.0,
            "energie_non_evacuee_moyenne_printemps": 222.0,
            "energie_non_evacuee_moyenne_ete": 2_150.0,
            "energie_non_evacuee_moyenne_automne": 155.0,
            "energie_non_evacuee_moyenne_hiver": 250.0,
        },
        {
            "region": "ÎLE-DE-FRANCE",
            "puissance_enr_installee": 786.0,
            "puissance_totale_a_compenser": 0.0,
            "energie_non_evacuee_moyenne_printemps": 0.0,
            "energie_non_evacuee_moyenne_ete": 0.0,
            "energie_non_evacuee_moyenne_automne": 0.0,
            "energie_non_evacuee_moyenne_hiver": 0.0,
        },
    ]


def load_bronze(
    database: Path, tmp_path: Path, spec: DatasetSpec, records: list[dict[str, Any]]
) -> None:
    ingested_at = datetime(2026, 7, 1, 3, 0, tzinfo=UTC)
    table = records_to_table(records, spec=spec, ingested_at=ingested_at)
    path = write_parquet(
        table,
        dataset_dir=spec.bronze_dataset_dir(tmp_path / "bronze"),
        run_id="test",
        ingest_date=ingested_at.date(),
    )
    with DuckDBLoader(database, spec=spec) as loader:
        loader.merge_parquet(path)


def dbt_build(database: Path, tmp_path: Path) -> None:
    assert DBT_BIN is not None, "dbt non installé"
    result = subprocess.run(
        [
            DBT_BIN, "build",
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
            "--target-path", str(tmp_path / "target"),
            "--log-path", str(tmp_path / "logs"),
        ],
        env={**os.environ, "ECO2MIX_DUCKDB_PATH": str(database)},
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )  # fmt: skip
    assert result.returncode == 0, f"dbt build en échec :\n{result.stdout}\n{result.stderr}"


def build_warehouse(tmp_path: Path, *, with_tempo: bool = True) -> Path:
    """Bronze chargé par l'ingestion, silver et gold construits par `dbt build`."""
    database = tmp_path / "eco2mix.duckdb"
    load_bronze(database, tmp_path, NATIONAL_CONS_DEF, consolide())
    load_bronze(database, tmp_path, NATIONAL_TR, temps_reel())
    if with_tempo:
        load_bronze(database, tmp_path, RTE_TEMPO, tempo())
    load_bronze(database, tmp_path, CONSO_IRIS, conso_iris())
    load_bronze(database, tmp_path, EQUILIBRE_REGIONAL, equilibre_regional())
    load_bronze(database, tmp_path, CONTRAINTES_REGIONALES, contraintes_regionales())
    ensure_bronze_tables(settings=Settings(_env_file=None, duckdb_path=database))
    dbt_build(database, tmp_path)
    return database
