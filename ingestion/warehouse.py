"""Chargement du Parquet brut dans le warehouse, derrière une interface stable.

Le reste du pipeline ne connaît que `WarehouseLoader`. `DuckDBLoader` en est
l'implémentation locale ; un `BigQueryLoader` viendra s'ajouter sans toucher au
DAG ni au client d'extraction, parce que le contrat est volontairement aligné
sur ce que les deux moteurs savent faire nativement :

* charger **un fichier Parquet désigné par un chemin ou une URI**
  (`read_parquet(...)` côté DuckDB, `LOAD DATA` / table externe côté BigQuery) ;
* fusionner sur la clé `date_heure` — jamais un `INSERT` simple, sans quoi le
  rejeu d'une fenêtre dupliquerait les lignes et les révisions de RTE
  s'empileraient au lieu de s'écraser.

Implémentation DuckDB du MERGE : `DELETE` des clés du lot puis `INSERT`, le tout
dans une transaction. Sémantiquement identique à un `MERGE ... WHEN MATCHED THEN
UPDATE`, mais portable sur toutes les versions de DuckDB. `BigQueryLoader`
utilisera l'instruction `MERGE` native, ce qui ne change rien à l'appelant.

Une instance de loader cible **un dataset** : la table, le schéma et la DDL sont
dérivés de son `DatasetSpec`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

import duckdb
import pyarrow as pa

from ingestion.config import Settings, get_settings
from ingestion.datasets import NATIONAL_TR, DatasetSpec

logger = logging.getLogger(__name__)

#: Couche médaillon alimentée par ce module. Silver et gold sont construites
#: par dbt (`dbt/`) à partir de ce schéma, jamais écrites depuis Python.
DEFAULT_SCHEMA = "bronze"

#: Correspondance des types pyarrow vers DuckDB. Le schéma du spec reste la
#: source de vérité unique : la DDL en est dérivée, jamais recopiée à la main.
_DUCKDB_TYPES: tuple[tuple[object, str], ...] = (
    (pa.timestamp("us", tz="UTC"), "TIMESTAMPTZ"),
    (pa.float64(), "DOUBLE"),
    (pa.string(), "VARCHAR"),
)


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Compte rendu d'un chargement, journalisé et remonté en XCom."""

    target: str
    rows_in_file: int
    rows_merged: int
    rows_inserted: int
    rows_updated: int

    @property
    def rows_deduplicated(self) -> int:
        """Lignes écartées parce qu'un même `date_heure` apparaissait deux fois dans le lot."""
        return self.rows_in_file - self.rows_merged

    def as_dict(self) -> dict[str, int | str]:
        return {
            "target": self.target,
            "rows_in_file": self.rows_in_file,
            "rows_merged": self.rows_merged,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
        }


@runtime_checkable
class WarehouseLoader(Protocol):
    """Contrat de chargement, indépendant du moteur de stockage."""

    spec: DatasetSpec

    def ensure_table(self) -> None:
        """Crée le schéma et la table cible s'ils n'existent pas."""
        ...

    def merge_parquet(self, parquet_path: Path | str) -> LoadResult:
        """Fusionne un fichier Parquet dans la table cible, sur la clé du spec."""
        ...

    def latest_timestamp(self, *, measure: str | None = None) -> datetime | None:
        """Plus récent `date_heure` présent, ou `None` si la table est vide.

        `measure` restreint aux lignes où cette colonne est renseignée : RTE
        publie l'horodatage le plus récent avant d'en avoir les mesures.
        """
        ...

    def count_distinct_instants(self, *, start: datetime, end: datetime) -> int:
        """Nombre d'instants distincts dans `[start, end)`.

        Réservé aux datasets de série temporelle : un dataset territorial n'a
        pas de colonne d'instant et l'appel échoue explicitement.
        """
        ...

    def row_count(self) -> int:
        """Nombre de lignes de la table cible."""
        ...

    def close(self) -> None:
        """Libère la connexion sous-jacente."""
        ...


def _duckdb_type(arrow_type: pa.DataType) -> str:
    for candidate, sql_type in _DUCKDB_TYPES:
        if arrow_type.equals(candidate):
            return sql_type
    raise TypeError(f"type pyarrow non pris en charge par la DDL DuckDB : {arrow_type}")


class DuckDBLoader:
    """Implémentation locale du contrat `WarehouseLoader`.

    DuckDB n'accepte qu'**un seul writer** sur un fichier : les DAGs sérialisent
    les tâches d'écriture (`max_active_runs=1` et pool `duckdb_writer` à 1 slot),
    et les lectures — contrôles qualité — ouvrent la base en `read_only`.
    """

    def __init__(
        self,
        database: Path | str,
        *,
        spec: DatasetSpec = NATIONAL_TR,
        schema: str = DEFAULT_SCHEMA,
        read_only: bool = False,
    ) -> None:
        self.database = database
        self.spec = spec
        self.schema = schema
        self.read_only = read_only
        self._connection: duckdb.DuckDBPyConnection | None = None

    # -- Fabriques ----------------------------------------------------------

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        *,
        spec: DatasetSpec = NATIONAL_TR,
        read_only: bool = False,
    ) -> DuckDBLoader:
        settings = settings or get_settings()
        return cls(database=settings.duckdb_path, spec=spec, read_only=read_only)

    # -- Cycle de vie -------------------------------------------------------

    @property
    def table(self) -> str:
        return self.spec.table_name

    @property
    def qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def _key_list(self) -> str:
        """Colonnes de clé, dans l'ordre du spec."""
        return ", ".join(self.spec.key_names)

    def _key_join(self, left: str, right: str) -> str:
        """Prédicat de jointure sur la clé composite entre deux alias."""
        return " AND ".join(f"{left}.{name} = {right}.{name}" for name in self.spec.key_names)

    @property
    def _dedup_order(self) -> str:
        """Ordre de départage des lignes d'une même clé dans un lot.

        La révision la plus récente gagne. À `ingested_at_utc` égal — deux lignes
        de même clé dans un même fichier — c'est la **plus complète** qui gagne :
        ODRÉ publie parfois un doublon dont toutes les mesures sont nulles, et un
        départage arbitraire ferait perdre la ligne renseignée une fois sur deux.
        """
        if not self.spec.measures:
            return "ingested_at_utc DESC"
        completude = " + ".join(
            f"CASE WHEN {column.name} IS NOT NULL THEN 1 ELSE 0 END"
            for column in self.spec.measures
        )
        return f"ingested_at_utc DESC, ({completude}) DESC"

    @property
    def _instant_column(self) -> str:
        """Colonne d'instant du spec, ou erreur si le dataset n'est pas temporel."""
        instant = self.spec.instant_column
        if instant is None:
            raise ValueError(
                f"{self.spec.dataset_id} n'est pas une serie temporelle : "
                "pas de controle temporel possible"
            )
        return instant

    @property
    def _column_list(self) -> str:
        """Colonnes du schéma, énumérées explicitement.

        Un `SELECT *` ramènerait aussi les colonnes de partition Hive déduites du
        chemin (`ingest_date=...`), absentes de la table cible.
        """
        return ", ".join(self.spec.schema.names)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Connexion paresseuse : rien n'est ouvert tant qu'aucune requête n'est émise."""
        if self._connection is None:
            if isinstance(self.database, Path) and not self.read_only:
                self.database.parent.mkdir(parents=True, exist_ok=True)
            self._connection = duckdb.connect(str(self.database), read_only=self.read_only)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> DuckDBLoader:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- Schéma -------------------------------------------------------------

    def ensure_table(self) -> None:
        """Crée la table cible à partir du schéma du spec, avec clé primaire.

        La clé primaire sur `date_heure` rend l'unicité structurelle : même si le
        code de fusion régressait, la base refuserait un doublon.
        """
        columns = ",\n    ".join(
            f"{field.name} {_duckdb_type(field.type)}" + ("" if field.nullable else " NOT NULL")
            for field in self.spec.schema
        )
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.qualified_table} (\n"
            f"    {columns},\n"
            f"    PRIMARY KEY ({self._key_list})\n"
            f")"
        )

    # -- Chargement ---------------------------------------------------------

    def merge_parquet(self, parquet_path: Path | str) -> LoadResult:
        """Fusionne le Parquet sur `date_heure` (idempotent, révisions écrasées).

        Trois étapes dans une seule transaction :

        1. dédoublonnage du lot (la révision la plus récente gagne, et à égalité
           d'horodatage la ligne la plus complète) ;
        2. suppression des clés du lot déjà présentes dans la cible ;
        3. insertion du lot.
        """
        self.ensure_table()
        source = str(parquet_path)
        connection = self.connection

        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE _batch AS
                SELECT {self._column_list}
                FROM read_parquet($source)
                QUALIFY row_number() OVER (
                    PARTITION BY {self._key_list} ORDER BY {self._dedup_order}
                ) = 1
                """,
                {"source": source},
            )
            rows_in_file = self._scalar(
                "SELECT count(*) FROM read_parquet($source)", {"source": source}
            )
            rows_merged = self._scalar("SELECT count(*) FROM _batch")
            rows_updated = self._scalar(
                f"""
                SELECT count(*) FROM {self.qualified_table} t
                WHERE EXISTS (SELECT 1 FROM _batch b WHERE {self._key_join("b", "t")})
                """
            )
            connection.execute(
                f"""
                DELETE FROM {self.qualified_table} t
                WHERE EXISTS (SELECT 1 FROM _batch b WHERE {self._key_join("b", "t")})
                """
            )
            connection.execute(f"INSERT INTO {self.qualified_table} BY NAME SELECT * FROM _batch")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.execute("DROP TABLE IF EXISTS _batch")

        result = LoadResult(
            target=self.qualified_table,
            rows_in_file=rows_in_file,
            rows_merged=rows_merged,
            rows_inserted=rows_merged - rows_updated,
            rows_updated=rows_updated,
        )
        logger.info("merge %s", result.as_dict())
        return result

    # -- Lecture ------------------------------------------------------------

    @property
    def _database_is_missing(self) -> bool:
        """Base absente ouverte en lecture seule : à traiter comme vide, pas comme une panne.

        C'est le cas au tout premier run, quand un contrôle qualité s'exécute
        avant qu'un chargement ait créé le fichier.
        """
        return self.read_only and isinstance(self.database, Path) and not self.database.exists()

    def latest_timestamp(self, *, measure: str | None = None) -> datetime | None:
        if self._database_is_missing or not self._table_exists():
            return None
        predicate = ""
        if measure is not None:
            # Le nom est interpole dans le SQL : on le valide contre le schema.
            if measure not in self.spec.schema.names:
                raise ValueError(f"colonne inconnue : {measure}")
            predicate = f" WHERE {measure} IS NOT NULL"
        value = self._scalar(
            f"SELECT max({self._instant_column}) FROM {self.qualified_table}{predicate}"
        )
        if value is None:
            return None
        # DuckDB renvoie un datetime aware ; on force UTC pour comparer sans surprise.
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)

    def count_distinct_instants(self, *, start: datetime, end: datetime) -> int:
        instant = self._instant_column
        if self._database_is_missing or not self._table_exists():
            return 0
        return self._scalar(
            f"""
            SELECT count(DISTINCT {instant}) FROM {self.qualified_table}
            WHERE {instant} >= $start AND {instant} < $end
            """,
            {"start": start, "end": end},
        )

    def row_count(self) -> int:
        if self._database_is_missing or not self._table_exists():
            return 0
        return self._scalar(f"SELECT count(*) FROM {self.qualified_table}")

    # -- Interne ------------------------------------------------------------

    def _table_exists(self) -> bool:
        return bool(
            self._scalar(
                """
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = $schema AND table_name = $table
                """,
                {"schema": self.schema, "table": self.table},
            )
        )

    def _scalar(self, query: str, parameters: dict[str, object] | None = None):
        row = self.connection.execute(query, parameters or {}).fetchone()
        return None if row is None else row[0]


def build_loader(
    settings: Settings | None = None,
    *,
    spec: DatasetSpec = NATIONAL_TR,
    read_only: bool = False,
) -> WarehouseLoader:
    """Fabrique le loader correspondant au backend configuré.

    Point d'extension unique pour la migration : ajouter `bigquery` ici suffira,
    aucun appelant n'a à changer.
    """
    settings = settings or get_settings()
    if settings.warehouse_backend == "duckdb":
        return DuckDBLoader.from_settings(settings, spec=spec, read_only=read_only)
    raise NotImplementedError(f"backend warehouse inconnu : {settings.warehouse_backend}")
