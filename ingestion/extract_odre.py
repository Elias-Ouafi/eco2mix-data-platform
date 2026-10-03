"""Client de l'API Explore v2.1 d'Opendatasoft, pour le dataset éCO2mix d'ODRÉ.

Points de conception :

* **Deux endpoints, une seule méthode.** `fetch_window()` choisit entre
  `/records` (paginé, plafonné par l'API) et `/exports/csv` (un seul appel, pas
  de plafond) selon le volume estimé de la fenêtre. L'appelant n'a pas à savoir
  lequel est utilisé.
* **Quota.** L'API ODRÉ est limitée à 50 000 appels par dataset et par mois.
  Chaque requête HTTP réellement émise — y compris les tentatives de retry —
  incrémente `client.calls`. Le quota restant annoncé par l'API dans ses
  en-têtes `X-RateLimit-dataset-*` est relevé au passage : les deux chiffres
  sont journalisés en fin de run.
* **Robustesse.** Timeout explicite, retry exponentiel sur les erreurs réseau et
  les statuts transitoires (429, 5xx). Une 4xx définitive n'est jamais rejouée.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import Settings, get_settings

logger = logging.getLogger(__name__)

#: Le dataset national temps réel est publié au pas 15 minutes : 4 lignes/heure.
#: Sert uniquement à estimer le volume d'une fenêtre pour choisir l'endpoint ;
#: une valeur trop haute est sans danger (on bascule sur l'export plus tôt).
EXPECTED_ROWS_PER_HOUR = 4

#: Taille de page maximale de `/records` en API v2.1.
RECORDS_PAGE_SIZE = 100

#: `offset` maximal accepté par `/records` (limit + offset <= 10 000).
RECORDS_MAX_OFFSET = 10_000 - RECORDS_PAGE_SIZE

#: Statuts HTTP considérés comme transitoires, donc rejouables.
RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})

USER_AGENT = "eco2mix-data-platform/0.1 (+https://github.com/)"


class ExtractionError(RuntimeError):
    """Erreur non récupérable pendant l'extraction."""


class PaginationLimitExceededError(ExtractionError):
    """La fenêtre demandée dépasse la profondeur de pagination de `/records`."""


class RetryableStatusError(ExtractionError):
    """Statut HTTP transitoire : la requête peut être rejouée."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"statut HTTP transitoire {status_code}")
        self.status_code = status_code


class ApiCallCounter:
    """Compteur d'appels HTTP, pour suivre la consommation du quota mensuel.

    Deux sources : nos propres appels (`count`, retries compris) et le compteur
    d'ODRÉ lui-même, renvoyé en en-tête de chaque réponse. Le second est
    l'autorité — il agrège tous les clients derrière la même adresse.
    """

    REMAINING_HEADER = "X-RateLimit-dataset-Remaining"
    LIMIT_HEADER = "X-RateLimit-dataset-Limit"
    RESET_HEADER = "X-RateLimit-dataset-Reset"

    def __init__(self) -> None:
        self.count = 0
        self.quota_remaining: int | None = None
        self.quota_limit: int | None = None
        self.quota_reset: str | None = None

    def increment(self) -> int:
        self.count += 1
        return self.count

    def record_quota(self, headers: Mapping[str, str]) -> None:
        """Mémorise le quota restant annoncé par l'API, si l'en-tête est présent."""
        self.quota_remaining = _optional_int(headers.get(self.REMAINING_HEADER))
        self.quota_limit = _optional_int(headers.get(self.LIMIT_HEADER))
        self.quota_reset = headers.get(self.RESET_HEADER) or None

    def __int__(self) -> int:
        return self.count

    def __repr__(self) -> str:  # pragma: no cover - confort de debug
        return f"ApiCallCounter(count={self.count}, quota_remaining={self.quota_remaining})"


def _optional_int(value: str | None) -> int | None:
    try:
        return None if value is None else int(value)
    except ValueError:  # en-tête présent mais illisible : on ne casse pas le run
        return None


@dataclass(frozen=True, slots=True)
class ExtractionWindow:
    """Fenêtre temporelle `[start, end)` exprimée en UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("les bornes de la fenêtre doivent être timezone-aware")
        if self.start >= self.end:
            raise ValueError(f"fenêtre vide ou inversée : {self.start} -> {self.end}")

    @classmethod
    def last_hours(cls, hours: int, *, now: datetime | None = None) -> ExtractionWindow:
        """Fenêtre glissante des `hours` dernières heures, bornée à maintenant."""
        end = (now or datetime.now(UTC)).astimezone(UTC)
        return cls(start=end - timedelta(hours=hours), end=end)

    @property
    def duration_hours(self) -> float:
        return (self.end - self.start).total_seconds() / 3600

    def estimated_rows(self, rows_per_hour: int = EXPECTED_ROWS_PER_HOUR) -> int:
        """Majorant du nombre de lignes attendues, utilisé pour choisir l'endpoint."""
        return int(self.duration_hours * rows_per_hour) + 1

    def to_odsql(self, column: str = "date_heure") -> str:
        """Traduit la fenêtre en filtre ODSQL (`where=`), bornes en UTC explicites."""
        start = self.start.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = self.end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{column} >= date'{start}' AND {column} < date'{end}'"

    def __str__(self) -> str:
        return f"[{self.start.isoformat()} -> {self.end.isoformat()})"


class OdreClient:
    """Client HTTP du dataset éCO2mix, utilisable comme context manager."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        dataset_id: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        #: Un client cible un dataset ; par defaut celui du temps reel.
        self.dataset_id = dataset_id or self.settings.dataset_id
        self.calls = ApiCallCounter()
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=self.settings.api_base_url,
            timeout=self.settings.http_timeout_seconds,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._retrying = Retrying(
            stop=stop_after_attempt(self.settings.max_retries),
            wait=wait_exponential(
                multiplier=self.settings.retry_wait_seconds,
                max=max(self.settings.retry_wait_seconds * 30, 1),
            ),
            retry=retry_if_exception_type((httpx.TransportError, RetryableStatusError)),
            reraise=True,
        )

    # -- Cycle de vie -------------------------------------------------------

    def close(self) -> None:
        """Ferme le client HTTP et journalise la consommation du quota."""
        logger.info(
            "odre_api_calls=%d dataset=%s quota_remaining=%s/%s quota_reset=%s",
            self.calls.count,
            self.dataset_id,
            self.calls.quota_remaining,
            self.calls.quota_limit,
            self.calls.quota_reset,
        )
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> OdreClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- Extraction ---------------------------------------------------------

    def fetch_window(self, window: ExtractionWindow) -> list[dict[str, Any]]:
        """Récupère tous les enregistrements de la fenêtre, endpoint choisi automatiquement."""
        estimated = window.estimated_rows()
        if estimated > self.settings.records_row_threshold:
            logger.info("fenetre %s (~%d lignes) : bascule sur /exports/csv", window, estimated)
            return self.fetch_export_csv(window)
        logger.info("fenetre %s (~%d lignes) : /records", window, estimated)
        return self.fetch_records(window)

    def fetch_records(self, window: ExtractionWindow) -> list[dict[str, Any]]:
        """Pagine `/records` jusqu'à épuisement de la fenêtre.

        Lève `PaginationLimitExceededError` si la fenêtre dépasse la profondeur
        maximale de l'endpoint : c'est le signal qu'il faut passer par l'export.
        """
        path = f"catalog/datasets/{self.dataset_id}/records"
        records: list[dict[str, Any]] = []
        offset = 0

        while True:
            payload = self._get(
                path,
                params={
                    "where": window.to_odsql(),
                    "order_by": "date_heure",
                    "limit": RECORDS_PAGE_SIZE,
                    "offset": offset,
                    "timezone": "UTC",
                },
            ).json()
            page = payload.get("results", [])
            records.extend(page)

            if len(page) < RECORDS_PAGE_SIZE:
                return records

            offset += RECORDS_PAGE_SIZE
            if offset > RECORDS_MAX_OFFSET:
                raise PaginationLimitExceededError(
                    f"fenetre {window} au-dela de la profondeur de /records "
                    f"(offset max {RECORDS_MAX_OFFSET}) : utiliser /exports/csv"
                )

    def fetch_export_csv(self, window: ExtractionWindow) -> list[dict[str, Any]]:
        """Récupère la fenêtre en un seul appel via `/exports/csv`.

        Les valeurs sont des chaînes (les vides valent `null`) ; `transform`
        se charge du typage.
        """
        path = f"catalog/datasets/{self.dataset_id}/exports/csv"
        response = self._get(
            path,
            params={
                "where": window.to_odsql(),
                "order_by": "date_heure",
                "timezone": "UTC",
                "delimiter": ";",
                "use_labels": "false",
            },
        )
        reader = csv.DictReader(io.StringIO(response.text), delimiter=";")
        return [dict(row) for row in reader]

    # -- Couche HTTP --------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any]) -> httpx.Response:
        """GET avec comptage d'appels et retry exponentiel sur erreurs transitoires."""

        def _send() -> httpx.Response:
            self.calls.increment()
            response = self._client.get(path, params=params)
            self.calls.record_quota(response.headers)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableStatusError(response.status_code)
            response.raise_for_status()
            return response

        send: Callable[[], httpx.Response] = _send
        return self._retrying(send)
