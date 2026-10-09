"""Description des datasets ingérés, source de vérité unique du schéma.

Un `DatasetSpec` porte tout ce qui distingue un dataset d'un autre : son
identifiant ODRÉ, sa table cible, et ses colonnes **typées par nature**. Le reste
du code — extraction, typage Parquet, DDL DuckDB, MERGE, DAGs — est écrit une
seule fois et paramétré par le spec, si bien qu'ajouter un dataset est déclaratif.

Les listes de colonnes ci-dessous ont été relevées sur l'API
(`GET /catalog/datasets/{id}`), pas devinées. Une colonne du spec absente de la
réponse est remplie à `null` et signalée dans les logs ; une colonne renvoyée par
l'API mais absente du spec est ignorée — c'est ainsi que les artefacts ODRÉ du
type `column_30` sont écartés.

**Clé de MERGE composite.** Tous les datasets ne sont pas des séries temporelles :
une consommation annuelle par IRIS est identifiée par `(annee, code_iris)`, un
équilibre régional par `(mois, code_insee_region)`, et un relevé de contraintes
par la seule `region`. Chaque spec déclare donc sa clé, et l'idempotence du
chargement porte sur ce tuple.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pyarrow as pa

#: Nom conventionnel de la colonne d'instant dans les séries temporelles éCO2mix.
#: Les contrôles temporels (fraîcheur, couverture mensuelle) s'y réfèrent.
KEY_COLUMN = "date_heure"

#: Colonnes techniques ajoutées par l'ingestion, communes à tous les specs.
INGESTED_AT_COLUMN = "ingested_at_utc"
SOURCE_COLUMN = "source_dataset"


class ColumnKind(StrEnum):
    """Nature d'une colonne, qui détermine son type et sa conversion."""

    #: Instant daté, normalisé en UTC à l'extraction.
    INSTANT = "instant"
    #: Période publiée telle quelle (« 2021 », « 2014-06 »). Conservée en texte :
    #: la couche bronze reste le miroir de la source, c'est à dbt de caster.
    PERIOD = "period"
    #: Code d'identification (INSEE, IRIS) — texte, jamais un nombre.
    CODE = "code"
    #: Libellé descriptif.
    LABEL = "label"
    #: Mesure numérique. Toujours `float64`, jamais entière : l'API renvoie des
    #: nulls, des chaînes via l'export CSV, et type même certaines colonnes
    #: numériques en `text` (`eolien`, `gaz_cogen`).
    MEASURE = "measure"


_ARROW_TYPES: dict[ColumnKind, pa.DataType] = {
    ColumnKind.INSTANT: pa.timestamp("us", tz="UTC"),
    ColumnKind.PERIOD: pa.string(),
    ColumnKind.CODE: pa.string(),
    ColumnKind.LABEL: pa.string(),
    ColumnKind.MEASURE: pa.float64(),
}


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    kind: ColumnKind

    @property
    def arrow_type(self) -> pa.DataType:
        return _ARROW_TYPES[self.kind]


# Constructeurs courts, pour que les specs se lisent comme une déclaration.
def instant(name: str) -> Column:
    return Column(name, ColumnKind.INSTANT)


def period(name: str) -> Column:
    return Column(name, ColumnKind.PERIOD)


def code(name: str) -> Column:
    return Column(name, ColumnKind.CODE)


def label(name: str) -> Column:
    return Column(name, ColumnKind.LABEL)


def measures(*names: str) -> tuple[Column, ...]:
    return tuple(Column(name, ColumnKind.MEASURE) for name in names)


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Identité, schéma, clé de MERGE et destination d'un dataset."""

    dataset_id: str
    #: Clé d'unicité, ordonnée. Porte la clé primaire et le MERGE.
    keys: tuple[Column, ...]
    #: Colonnes descriptives non clés.
    attributes: tuple[Column, ...]
    #: Mesures.
    measures: tuple[Column, ...]
    description: str

    def __post_init__(self) -> None:
        if not self.keys:
            raise ValueError(f"{self.dataset_id} : au moins une colonne de clé est requise")
        if len([c for c in self.keys if c.kind is ColumnKind.INSTANT]) > 1:
            raise ValueError(f"{self.dataset_id} : une seule colonne d'instant est acceptée")
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"{self.dataset_id} : colonnes en double")

    # -- Identité -----------------------------------------------------------

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

    # -- Colonnes -----------------------------------------------------------

    @property
    def columns(self) -> tuple[Column, ...]:
        return self.keys + self.attributes + self.measures

    @property
    def key_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.keys)

    @property
    def instant_column(self) -> str | None:
        """Colonne d'instant de la clé, si le dataset est une série temporelle.

        `None` pour les datasets territoriaux : les contrôles de fraîcheur et de
        couverture mensuelle ne s'y appliquent pas.
        """
        return next((c.name for c in self.keys if c.kind is ColumnKind.INSTANT), None)

    @property
    def is_time_series(self) -> bool:
        return self.instant_column is not None

    @property
    def schema(self) -> pa.Schema:
        """Schéma Parquet, dont la DDL du warehouse est dérivée.

        Les colonnes de clé sont non nullables — c'est la garantie structurelle
        de l'idempotence du MERGE.
        """
        fields = [pa.field(c.name, c.arrow_type, nullable=False) for c in self.keys]
        fields += [pa.field(c.name, c.arrow_type) for c in self.attributes + self.measures]
        fields += [
            pa.field(INGESTED_AT_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field(SOURCE_COLUMN, pa.string(), nullable=False),
        ]
        return pa.schema(fields)


# ---------------------------------------------------------------------------
# Datasets nationaux (séries temporelles au pas 15 min)
# ---------------------------------------------------------------------------

_NATIONAL_LABELS = (label("perimetre"), label("nature"))

_COMMON_MEASURES = measures(
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
_CONS_DEF_DETAIL = measures(
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

#: Temps réel : publié au pas 15 min avec quelques minutes de délai, révisé en continu.
NATIONAL_TR = DatasetSpec(
    dataset_id="eco2mix-national-tr",
    keys=(instant(KEY_COLUMN),),
    attributes=_NATIONAL_LABELS,
    measures=_COMMON_MEASURES,
    description="éCO2mix national temps réel",
)

#: Consolidé puis définitif : publié avec plusieurs mois de retard, et rejoué
#: chaque année quand une tranche passe de « consolidées » à « définitives ».
NATIONAL_CONS_DEF = DatasetSpec(
    dataset_id="eco2mix-national-cons-def",
    keys=(instant(KEY_COLUMN),),
    attributes=_NATIONAL_LABELS,
    measures=_COMMON_MEASURES + _CONS_DEF_DETAIL,
    description="éCO2mix national consolidé et définitif",
)


# ---------------------------------------------------------------------------
# Datasets territoriaux (diagnostic d'implantation)
# ---------------------------------------------------------------------------

#: Mix régional consolidé/définitif, au pas 15 min et par région.
#: Clé composite : un instant vaut pour les 12 régions à la fois.
#: À noter : ce dataset **ne publie pas de `taux_co2`** — il n'existe pas
#: d'intensité carbone régionale officielle (cf. docs/cadrage-besoins-entreprises.md).
REGIONAL_CONS_DEF = DatasetSpec(
    dataset_id="eco2mix-regional-cons-def",
    keys=(instant(KEY_COLUMN), code("code_insee_region")),
    attributes=(label("libelle_region"), label("nature")),
    measures=measures(
        "consommation",
        "thermique",
        "nucleaire",
        "eolien",
        "eolien_terrestre",
        "eolien_offshore",
        "solaire",
        "hydraulique",
        "pompage",
        "bioenergies",
        "ech_physiques",
        "stockage_batterie",
        "destockage_batterie",
        "tco_thermique",
        "tch_thermique",
        "tco_nucleaire",
        "tch_nucleaire",
        "tco_eolien",
        "tch_eolien",
        "tco_solaire",
        "tch_solaire",
        "tco_hydraulique",
        "tch_hydraulique",
        "tco_bioenergies",
        "tch_bioenergies",
    ),
    description="éCO2mix régional consolidé et définitif",
)

#: Pièce maîtresse du diagnostic : consommation annuelle des sites industriels
#: raccordés au réseau de transport, à la maille IRIS. Se joint au géocodage BAN
#: par `code_insee_commune`.
CONSO_IRIS = DatasetSpec(
    dataset_id="consommation-annuelle-par-iris",
    keys=(period("annee"), code("code_iris")),
    attributes=(
        code("code_insee_commune"),
        label("commune"),
        code("code_insee_departement"),
        label("departement"),
        code("code_insee_region"),
        label("region"),
    ),
    measures=measures(
        "consommation_electricite_rte",
        "pdl_electricite_rte",
        "consommation_gaz_grtgaz",
        "pdl_gaz_grtgaz",
        "consommation_gaz_terega",
        "pdl_gaz_terega",
        "consommation_totale",
        "pdl_total",
    ),
    description="Consommation annuelle des sites industriels raccordés au transport, par IRIS",
)

#: Région excédentaire ou déficitaire, mois par mois.
EQUILIBRE_REGIONAL = DatasetSpec(
    dataset_id="equilibre-regional-mensuel-prod-conso-brute",
    keys=(period("mois"), code("code_insee_region")),
    attributes=(label("region"),),
    measures=measures(
        "production_totale",
        "pompage",
        "solde_echanges_physiques",
        "consommation_brute",
    ),
    description="Équilibre mensuel production / consommation brute par région",
)

#: Signal de contrainte du réseau régional : puissance à compenser et énergie
#: non évacuée par saison. Instantané sans dimension temporelle (12 lignes).
CONTRAINTES_REGIONALES = DatasetSpec(
    dataset_id="energies-et-puissances-regionales-liees-au-contraintes",
    keys=(code("region"),),
    attributes=(),
    measures=measures(
        "puissance_enr_installee",
        "puissance_totale_a_compenser",
        "energie_non_evacuee_moyenne_printemps",
        "energie_non_evacuee_moyenne_ete",
        "energie_non_evacuee_moyenne_automne",
        "energie_non_evacuee_moyenne_hiver",
    ),
    description="Énergies et puissances régionales liées aux contraintes réseau",
)


# ---------------------------------------------------------------------------
# Autres sources
# ---------------------------------------------------------------------------

#: Calendrier Tempo, extrait de l'API RTE (pas d'ODRÉ) : une couleur par jour.
RTE_TEMPO = DatasetSpec(
    dataset_id="rte-tempo",
    keys=(instant(KEY_COLUMN),),
    attributes=(label("couleur"),),
    measures=(),
    description="Calendrier Tempo RTE (couleur de chaque jour)",
)


# ---------------------------------------------------------------------------
# Registres
# ---------------------------------------------------------------------------

#: Datasets extraits depuis ODRÉ, adressables par `--dataset` en ligne de commande.
SPECS: dict[str, DatasetSpec] = {
    spec.dataset_id: spec
    for spec in (
        NATIONAL_TR,
        NATIONAL_CONS_DEF,
        REGIONAL_CONS_DEF,
        CONSO_IRIS,
        EQUILIBRE_REGIONAL,
        CONTRAINTES_REGIONALES,
    )
}

#: Datasets du diagnostic territorial.
TERRITORIAL_SPECS: tuple[DatasetSpec, ...] = (
    CONSO_IRIS,
    EQUILIBRE_REGIONAL,
    CONTRAINTES_REGIONALES,
)

#: Tables bronze créées d'office à chaque chargement, parce que la couche silver
#: (dbt) lit toutes ses sources à chaque run. Sans cela, un run échouerait tant
#: qu'un autre DAG n'a pas créé sa table — une dépendance d'ordre de déploiement
#: qu'aucun DAG ne devrait porter.
BRONZE_SPECS: tuple[DatasetSpec, ...] = (
    NATIONAL_TR,
    NATIONAL_CONS_DEF,
    RTE_TEMPO,
    *TERRITORIAL_SPECS,
)


def get_spec(dataset_id: str) -> DatasetSpec:
    """Retrouve un spec par identifiant ODRÉ."""
    try:
        return SPECS[dataset_id]
    except KeyError:
        known = ", ".join(sorted(SPECS))
        raise KeyError(f"dataset inconnu : {dataset_id} (connus : {known})") from None
