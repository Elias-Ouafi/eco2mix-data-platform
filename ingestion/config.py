"""Configuration du socle d'ingestion, lue depuis l'environnement.

Toutes les variables sont préfixées `ECO2MIX_` (voir `.env.example`). L'API
ODRÉ est publique et anonyme ; seuls les identifiants de l'API RTE (calendrier
Tempo) sont des secrets, typés `SecretStr` pour ne jamais apparaître dans un
log. Les chemins sont résolus relativement au répertoire courant, ce qui permet
de pointer sur le volume monté `/usr/local/airflow/data` dans les conteneurs
Astro sans changer de code.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Paramètres du pipeline d'ingestion."""

    model_config = SettingsConfigDict(
        env_prefix="ECO2MIX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Sources -----------------------------------------------------------
    api_base_url: str = "https://odre.opendatasoft.com/api/explore/v2.1"

    #: Dataset temps réel, ingéré toutes les heures.
    dataset_id: str = "eco2mix-national-tr"

    #: Dataset consolidé puis définitif, ingéré tous les mois.
    consolidation_dataset_id: str = "eco2mix-national-cons-def"

    # --- RTE Data (calendrier Tempo) --------------------------------------
    #: Portail API de RTE. Compte gratuit sur https://data.rte-france.com, puis
    #: abonnement à l'API « Tempo Like Supply Contract » pour obtenir ces identifiants.
    rte_api_base_url: str = "https://digital.iservices.rte-france.com"

    #: API Adresse de la Géoplateforme (IGN). L'ancien point d'accès
    #: api-adresse.data.gouv.fr a été décommissionné fin janvier 2026.
    ban_base_url: str = "https://data.geopf.fr/geocodage"
    rte_client_id: str | None = None
    rte_client_secret: SecretStr | None = None

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
    #: Couche bronze : un Parquet immuable par exécution, fidèle à la source.
    #: Les couches silver et gold vivent dans DuckDB et sont construites par dbt.
    bronze_dir: Path = Path("data/bronze")
    duckdb_path: Path = Path("data/warehouse/eco2mix.duckdb")

    #: Rapports PDF générés à la demande (DAG `eco2mix_monthly_report`).
    reports_dir: Path = Path("data/reports")

    #: Backend de chargement. `bigquery` sera ajouté lors de la migration cloud ;
    #: c'est le seul endroit du code où le moteur est nommé.
    warehouse_backend: Literal["duckdb"] = "duckdb"

    # --- Fenêtre de consolidation -----------------------------------------
    #: Profondeur relue à chaque run mensuel. 24 mois par défaut : RTE publie le
    #: consolidé avec plusieurs mois de retard, puis rejoue une année entière
    #: quand elle passe en « définitives ». Le MERGE rend ce recouvrement gratuit.
    consolidation_lookback_months: int = Field(default=24, ge=1)

    # --- Qualité -----------------------------------------------------------
    freshness_max_lag_hours: int = Field(default=2, ge=1)

    #: Part minimale des pas de temps attendus pour valider un mois consolidé.
    consolidation_min_coverage: float = Field(default=0.95, gt=0, le=1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Instance partagée, mise en cache pour éviter de relire `.env` à chaque appel."""
    return Settings()
