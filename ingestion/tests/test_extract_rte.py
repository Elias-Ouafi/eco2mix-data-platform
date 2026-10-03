"""Client de l'API RTE Tempo : authentification, plages, parsing. Aucun appel réseau."""

from __future__ import annotations

from datetime import date
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from pytest_httpx import HTTPXMock

from ingestion.config import Settings
from ingestion.extract_rte import (
    CALENDAR_PATH,
    TOKEN_PATH,
    DayRange,
    RteCredentialsMissingError,
    RteTempoClient,
)

RTE_BASE = "https://rte.test"
TOKEN_URL = f"{RTE_BASE}{TOKEN_PATH}"
TODAY = date(2026, 10, 3)


@pytest.fixture
def rte_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={
            "rte_api_base_url": RTE_BASE,
            "rte_client_id": "client-test",
            "rte_client_secret": SecretStr("secret-test"),
        }
    )


def _calendar(*days: tuple[str, str]) -> dict[str, Any]:
    """Réponse au format RTE, jours du plus récent au plus ancien comme l'API."""
    values = [
        {
            "start_date": f"{start}T00:00:00+02:00",
            "end_date": f"{start}T23:59:59+02:00",
            "value": color,
            "updated_date": f"{start}T10:20:00+02:00",
        }
        for start, color in reversed(days)
    ]
    return {"tempo_like_calendars": {"values": values}}


def _is_calendar(request: httpx.Request) -> bool:
    return request.url.path == CALENDAR_PATH


def test_missing_credentials_fail_with_an_actionable_message(settings: Settings) -> None:
    with pytest.raises(RteCredentialsMissingError, match="ECO2MIX_RTE_CLIENT_ID"):
        RteTempoClient(settings)


def test_calendar_is_parsed_and_sorted(httpx_mock: HTTPXMock, rte_settings: Settings) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(
        json=_calendar(("2026-09-01", "BLUE"), ("2026-09-02", "WHITE"), ("2026-09-03", "RED"))
    )

    with RteTempoClient(rte_settings) as client:
        records = client.fetch_calendar(DayRange(date(2026, 9, 1), date(2026, 9, 4)), today=TODAY)

    assert records == [
        {"date_heure": "2026-09-01T00:00:00+02:00", "couleur": "BLUE"},
        {"date_heure": "2026-09-02T00:00:00+02:00", "couleur": "WHITE"},
        {"date_heure": "2026-09-03T00:00:00+02:00", "couleur": "RED"},
    ]
    calendar_request = next(r for r in httpx_mock.get_requests() if _is_calendar(r))
    assert calendar_request.headers["Authorization"] == "Bearer jeton"
    # Bornes en minuit heure de Paris, avec décalage explicite.
    assert calendar_request.url.params["start_date"] == "2026-09-01T00:00:00+02:00"
    assert calendar_request.url.params["end_date"] == "2026-09-04T00:00:00+02:00"


def test_token_request_uses_basic_auth(httpx_mock: HTTPXMock, rte_settings: Settings) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(json=_calendar(("2026-09-01", "BLUE")))

    with RteTempoClient(rte_settings) as client:
        client.fetch_calendar(DayRange(date(2026, 9, 1), date(2026, 9, 2)), today=TODAY)

    token_request = httpx_mock.get_requests(url=TOKEN_URL)[0]
    assert token_request.headers["Authorization"].startswith("Basic ")
    assert token_request.content == b"grant_type=client_credentials"


def test_long_ranges_are_split_and_token_is_reused(
    httpx_mock: HTTPXMock, rte_settings: Settings
) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(json=_calendar(("2024-01-01", "BLUE")), is_reusable=True)

    with RteTempoClient(rte_settings) as client:
        client.fetch_calendar(DayRange(date(2024, 1, 1), date(2026, 1, 1)), today=TODAY)

    # 731 jours -> 2 tranches de 366 jours au plus, un seul jeton pour les deux.
    assert len([r for r in httpx_mock.get_requests() if _is_calendar(r)]) == 2
    assert len(httpx_mock.get_requests(url=TOKEN_URL)) == 1


def test_future_days_are_not_requested(httpx_mock: HTTPXMock, rte_settings: Settings) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(json=_calendar(("2026-10-03", "BLUE"), ("2026-10-04", "BLUE")))

    with RteTempoClient(rte_settings) as client:
        client.fetch_calendar(DayRange(date(2026, 10, 1), date(2026, 11, 1)), today=TODAY)

    calendar_request = next(r for r in httpx_mock.get_requests() if _is_calendar(r))
    # La couleur de J+1 est la plus lointaine publiée : la plage s'arrête à J+2 exclu.
    assert calendar_request.url.params["end_date"] == "2026-10-05T00:00:00+02:00"


def test_entirely_future_range_makes_no_call(rte_settings: Settings) -> None:
    with RteTempoClient(rte_settings) as client:
        records = client.fetch_calendar(DayRange(date(2027, 1, 1), date(2027, 2, 1)), today=TODAY)
    assert records == []
    assert client.calls == 0


def test_transient_errors_are_retried(httpx_mock: HTTPXMock, rte_settings: Settings) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(json=_calendar(("2026-09-01", "BLUE")))

    with RteTempoClient(rte_settings) as client:
        records = client.fetch_calendar(DayRange(date(2026, 9, 1), date(2026, 9, 2)), today=TODAY)

    assert len(records) == 1


def test_unknown_colors_are_dropped(httpx_mock: HTTPXMock, rte_settings: Settings) -> None:
    httpx_mock.add_response(url=TOKEN_URL, json={"access_token": "jeton", "expires_in": 7200})
    httpx_mock.add_response(json=_calendar(("2026-09-01", "BLUE"), ("2026-09-02", "PURPLE")))

    with RteTempoClient(rte_settings) as client:
        records = client.fetch_calendar(DayRange(date(2026, 9, 1), date(2026, 9, 3)), today=TODAY)

    assert [r["couleur"] for r in records] == ["BLUE"]


def test_month_range_covers_every_day() -> None:
    assert DayRange.month(2026, 12) == DayRange(date(2026, 12, 1), date(2027, 1, 1))
