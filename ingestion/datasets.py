"""Description des datasets éCO2mix ingérés, source de vérité unique du schéma.

Un `DatasetSpec` porte tout ce qui distingue un dataset d'un autre : son
identifiant ODRÉ, sa table cible, ses colonnes. Le reste du code — extraction,
typage Parquet, DDL DuckDB, DAG — est écrit une seule fois et paramétré par le
spec, ce qui rend l'ajout d'un dataset (régional, par exemple) déclaratif.

Les listes de colonnes ci-dessous ont été relevées sur l'API
(`GET /catalog/datasets/{id}`), pas devinées. Une colonne du spec absente de la
réponse est remplie à `null` et signalée dans les logs ; une colonne renvoyée par
l'API mais absente du spec est ignorée.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

#: Colonne clé, commune à tous les datasets éCO2mix : c'est la clé du MERGE.
KEY_COLUMN = "date_heure"

#: Colonnes techniques ajoutées par l'ingestion, communes à tous les specs.
INGESTED_AT_COLUMN = "ingested_at_utc"
SOURCE_COLUMN = "source_dataset"

#: Colonnes descriptives, communes aux deux datasets nationaux.
_LABELS: tuple[str, ...] = ("perimetre", "nature")

#: Mesures publiées par les deux datasets nationaux.
_COMMON_MEASURES: tuple[str, ...] = (
    "consommation",
    "prevision_j1",
    "prevision_j",
    "fioul",
    "charbon",
    "gaz",
    "nucleaire",
    "eolien",
    "solaire",
    "hydraulique",
    "pompage",
    "bioenergies",
    "ech_physiques",
    "taux_co2",
    "ech_comm_angleterre",
    "ech_comm_espagne",
    "ech_comm_italie",
    "ech_comm_suisse",
    "ech_comm_allemagne_belgique",
)

#: Détail par filière, publié uniquement dans le dataset consolidé/définitif.
#: C'est la valeur ajoutée de ce dataset par rapport au temps réel.
_CONS_DEF_DETAIL: tuple[str, ...] = (
    "fioul_tac",
    "fioul_cogen",
    "fioul_autres",
    "gaz_tac",
    "gaz_cogen",
    "gaz_ccg",
    "gaz_autres",
    "hydraulique_fil_eau_eclusee",
    "hydraulique_lacs",
    "hydraulique_step_turbinage",
    "bioenergies_dechets",
    "bioenergies_biomasse",
    "bioenergies_biogaz",
)


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Identité, schéma et destination d'un dataset éCO2mix."""

    dataset_id: str
    label_columns: tuple[str, ...]
    measure_columns: tuple[str, ...]
    description: str

    @property
    def table_name(self) -> str:
        """Table cible (`eco2mix-national-tr` -> `national_tr`)."""
        return self.dataset_id.removeprefix("eco2mix-").replace("-", "_")

    @property
    def slug(self) -> str:
        """Nom de dossier de la couche bronze (`eco2mix_national_tr`)."""
        return self.dataset_id.replace("-", "_")

    def bronze_dataset_dir(self, bronze_dir: Path) -> Path:
        return bronze_dir / self.slug

    @property
    def schema(self) -> pa.Schema:
        """Schéma Parquet, dont la DDL du warehouse est dérivée.

        Les mesures sont typées `float64` et non entières : l'API renvoie des
        valeurs nulles sur les filières non renseignées, des chaînes via l'export
        CSV, et type même certaines colonnes en `text` côté ODRÉ.
        """
        return pa.schema(
            [pa.field(KEY_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False)]
            + [pa.field(name, pa.string()) for name in self.label_columns]
            + [pa.field(name, pa.float64()) for name in self.measure_columns]
            + [
                pa.field(INGESTED_AT_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field(SOURCE_COLUMN, pa.string(), nullable=False),
            ]
        )


#: Temps réel : publié au pas 15 min avec quelques minutes de délai, révisé en continu.
NATIONAL_TR = DatasetSpec(
    dataset_id="eco2mix-national-tr",
    label_columns=_LABELS,
    measure_columns=_COMMON_MEASURES,
    description="éCO2mix national temps réel",
)

#: Consolidé puis définitif : publié avec plusieurs mois de retard, et rejoué
#: chaque année quand une tranche passe de « consolidées » à « définitives ».
NATIONAL_CONS_DEF = DatasetSpec(
    dataset_id="eco2mix-national-cons-def",
    label_columns=_LABELS,
    measure_columns=_COMMON_MEASURES + _CONS_DEF_DETAIL,
    description="éCO2mix national consolidé et définitif",
)

#: Calendrier Tempo publié par RTE : une ligne par jour, clé = début du jour civil.
#: Hors ODRÉ (API RTE authentifiée), d'où son absence de `SPECS` : la CLI et les
#: DAGs éCO2mix ne doivent pas pouvoir l'extraire avec le client ODRÉ.
RTE_TEMPO = DatasetSpec(
    dataset_id="rte-tempo",
    label_columns=("couleur",),
    measure_columns=(),
    description="Calendrier Tempo RTE (couleur de chaque jour)",
)

#: Datasets éCO2mix extraits depuis ODRÉ.
SPECS: dict[str, DatasetSpec] = {spec.dataset_id: spec for spec in (NATIONAL_TR, NATIONAL_CONS_DEF)}

#: Toutes les tables de la couche bronze, quelle que soit leur source.
BRONZE_SPECS: tuple[DatasetSpec, ...] = (*SPECS.values(), RTE_TEMPO)


def get_spec(dataset_id: str) -> DatasetSpec:
    """Retrouve un spec par identifiant ODRÉ."""
    try:
        return SPECS[dataset_id]
    except KeyError:
        known = ", ".join(sorted(SPECS))
        raise KeyError(f"dataset inconnu : {dataset_id} (connus : {known})") from None
