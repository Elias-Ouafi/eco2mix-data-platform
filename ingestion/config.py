"""Configuration du socle d'ingestion, lue depuis l'environnement.

Toutes les variables sont préfixées `ECO2MIX_` (voir `.env.example`). Aucune
n'est un secret : l'API ODRÉ est publique et anonyme. Les chemins sont résolus
relativement au répertoire courant, ce qui permet de pointer sur le volume monté
`/usr/local/airflow/data` dans les conteneurs Astro sans changer de code.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Paramètres du pipeline d'ingestion."""

    model_config = SettingsConfigDict(
        env_prefix="ECO2MIX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Source ------------------------------------------------------------
    api_base_url: str = "https://odre.opendatasoft.com/api/explore/v2.1"
    dataset_id: str = "eco2mix-national-tr"

    # --- Fenêtre d'extraction ---------------------------------------------
    #: Profondeur de la fenêtre glissante par défaut. 3 h couvre le délai de
    #: publication de RTE et rattrape les valeurs révisées a posteriori.
    lookback_hours: int = Field(default=3, ge=1)

    # --- Réseau ------------------------------------------------------------
    http_timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=4, ge=1)

    #: Multiplicateur de l'attente exponentielle entre deux tentatives.
    #: Mis à 0 dans les tests pour ne pas ralentir la suite.
    retry_wait_seconds: float = Field(default=1.0, ge=0)

    #: Au-delà de ce nombre de lignes estimées, on abandonne la pagination de
    #: `/records` (plafonnée par l'API) au profit de `/exports/csv`.
    records_row_threshold: int = Field(default=900, ge=1)

    # --- Stockage local ----------------------------------------------------
    raw_dir: Path = Path("data/raw")
    duckdb_path: Path = Path("data/warehouse/eco2mix.duckdb")

    #: Backend de chargement. `bigquery` sera ajouté lors de la migration cloud ;
    #: c'est le seul endroit du code où le moteur est nommé.
    warehouse_backend: Literal["duckdb"] = "duckdb"

    # --- Qualité -----------------------------------------------------------
    freshness_max_lag_hours: int = Field(default=2, ge=1)

    @property
    def table_name(self) -> str:
        """Nom de table warehouse dérivé du dataset (`eco2mix-national-tr` -> `national_tr`)."""
        return self.dataset_id.removeprefix("eco2mix-").replace("-", "_")

    @property
    def raw_dataset_dir(self) -> Path:
        """Racine des partitions Parquet du dataset (`data/raw/eco2mix_national_tr`)."""
        return self.raw_dir / self.dataset_id.replace("-", "_")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Instance partagée, mise en cache pour éviter de relire `.env` à chaque appel."""
    return Settings()
