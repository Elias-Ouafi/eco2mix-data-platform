"""Client de l'API RTE « Tempo Like Supply Contract » : couleur Tempo de chaque jour.

Points de conception :

* **OAuth2 client credentials.** Un jeton est demandé au premier appel puis
  réutilisé jusqu'à son expiration (2 h chez RTE), avec une marge pour ne jamais
  émettre une requête avec un jeton sur le point d'expirer.
* **Fenêtres bornées.** L'API refuse les plages de plus d'un an et les dates au-delà
  du lendemain (la couleur de J+1 est publiée vers 11 h). Une fenêtre longue est
  découpée en tranches, une fenêtre future est tronquée à J+1 inclus.
* **Même robustesse que le client ODRÉ.** Timeout explicite, retry exponentiel sur
  erreurs réseau et statuts transitoires, jamais sur une 4xx définitive.

Documentation : https://data.rte-france.com/catalog/-/api/consumption/Tempo-Like-Supply-Contract/v1.1
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import Settings, get_settings
from ingestion.extract_odre import (
    RETRYABLE_STATUS_CODES,
    USER_AGENT,
    ExtractionError,
    RetryableStatusError,
)
from ingestion.transform import PARIS_TZ

logger = logging.getLogger(__name__)

TOKEN_PATH = "/token/oauth/"
CALENDAR_PATH = "/open_api/tempo_like_supply_contract/v1/tempo_like_calendars"

#: Plage maximale acceptée par l'API en un appel.
MAX_DAYS_PER_CALL = 366

#: Marge de renouvellement du jeton avant son expiration annoncée.
TOKEN_EXPIRY_MARGIN_SECONDS = 60

#: Couleurs publiées par RTE.
TEMPO_COLORS = frozenset({"BLUE", "WHITE", "RED"})


class RteCredentialsMissingError(ExtractionError):
    """Les identifiants de l'API RTE ne sont pas configurés."""


@dataclass(frozen=True, slots=True)
class DayRange:
    """Plage de jours civils `[start, end)`, en heure de Paris."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError(f"plage de jours vide ou inversée : {self.start} -> {self.end}")

    @classmethod
    def month(cls, year: int, month: int) -> DayRange:
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        return cls(start=date(year, month, 1), end=date(next_year, next_month, 1))

    def chunks(self, max_days: int = MAX_DAYS_PER_CALL) -> list[DayRange]:
        """Découpe la plage en tranches d'au plus `max_days` jours."""
        result = []
        current = self.start
        while current < self.end:
            stop = min(current + timedelta(days=max_days), self.end)
            result.append(DayRange(current, stop))
            current = stop
        return result

    def __str__(self) -> str:
        return f"[{self.start.isoformat()} -> {self.end.isoformat()})"


def _paris_midnight(day: date) -> str:
    """Minuit heure de Paris, au format ISO avec décalage attendu par l'API."""
    return datetime(day.year, day.month, day.day, tzinfo=PARIS_TZ).isoformat()


class RteTempoClient:
    """Client HTTP du calendrier Tempo, utilisable comme context manager."""

    def __init__(self, settings: Settings | None = None, *, client: httpx.Client | None = None):
        self.settings = settings or get_settings()
        if not (self.settings.rte_client_id and self.settings.rte_client_secret):
            raise RteCredentialsMissingError(
                "identifiants RTE absents : renseigner ECO2MIX_RTE_CLIENT_ID et "
                "ECO2MIX_RTE_CLIENT_SECRET (compte gratuit sur https://data.rte-france.com, "
                "API « Tempo Like Supply Contract »)"
            )
        self.calls = 0
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=self.settings.rte_api_base_url,
            timeout=self.settings.http_timeout_seconds,
            headers={"User-Agent": USER_AGENT},
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
        logger.info("rte_api_calls=%d", self.calls)
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> RteTempoClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- Extraction ---------------------------------------------------------

    def fetch_calendar(self, days: DayRange, *, today: date | None = None) -> list[dict[str, Any]]:
        """Couleur de chaque jour de la plage, sous forme d'enregistrements bronze.

        Les jours postérieurs à J+1 sont ignorés : leur couleur n'existe pas encore.
        Chaque enregistrement porte `date_heure` (début du jour civil) et `couleur`.
        """
        today = today or datetime.now(PARIS_TZ).date()
        horizon = today + timedelta(days=2)
        if days.start >= horizon:
            logger.warning("plage %s entièrement dans le futur : rien à extraire", days)
            return []
        if days.end > horizon:
            logger.info("plage %s tronquée à %s (couleurs non publiées)", days, horizon)
            days = DayRange(days.start, horizon)

        records: list[dict[str, Any]] = []
        for chunk in days.chunks():
            payload = self._get(
                CALENDAR_PATH,
                params={
                    "start_date": _paris_midnight(chunk.start),
                    "end_date": _paris_midnight(chunk.end),
                },
            ).json()
            values = payload.get("tempo_like_calendars", {}).get("values", [])
            for value in values:
                color = value.get("value")
                if color not in TEMPO_COLORS:
                    logger.warning("couleur Tempo inconnue ignorée : %r", value)
                    continue
                records.append({"date_heure": value["start_date"], "couleur": color})

        # L'API renvoie les jours du plus récent au plus ancien : on remet dans
        # l'ordre chronologique attendu par la normalisation des horodatages.
        records.sort(key=lambda record: record["date_heure"])
        return records

    # -- Couche HTTP --------------------------------------------------------

    def _access_token(self) -> str:
        if self._token is not None and time.monotonic() < self._token_expires_at:
            return self._token

        assert self.settings.rte_client_secret is not None  # vérifié à l'initialisation

        def _send() -> httpx.Response:
            self.calls += 1
            response = self._client.post(
                TOKEN_PATH,
                auth=(
                    self.settings.rte_client_id or "",
                    self.settings.rte_client_secret.get_secret_value(),
                ),
                data={"grant_type": "client_credentials"},
            )
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableStatusError(response.status_code)
            response.raise_for_status()
            return response

        payload = self._retrying(_send).json()
        self._token = payload["access_token"]
        lifetime = float(payload.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + lifetime - TOKEN_EXPIRY_MARGIN_SECONDS
        return self._token

    def _get(self, path: str, params: dict[str, Any]) -> httpx.Response:
        def _send() -> httpx.Response:
            token = self._access_token()
            self.calls += 1
            response = self._client.get(
                path, params=params, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 401:
                # Jeton révoqué ou expiré côté serveur : on en redemandera un.
                self._token = None
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableStatusError(response.status_code)
            response.raise_for_status()
            return response

        return self._retrying(_send)
