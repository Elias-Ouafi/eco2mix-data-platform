"""Client d'extraction : pagination, bascule vers l'export, retry, quota.

Tous les échanges HTTP sont simulés par `pytest-httpx`, qui intercepte au niveau
du transport : aucun octet ne sort de la machine.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from ingestion import extract_odre
from ingestion.config import Settings
from ingestion.extract_odre import (
    ExtractionWindow,
    OdreClient,
    PaginationLimitExceededError,
)

NOW = datetime(2026, 9, 7, 6, 0, tzinfo=UTC)


@pytest.fixture
def window() -> ExtractionWindow:
    """Fenêtre horaire par défaut : les 3 dernières heures."""
    return ExtractionWindow.last_hours(3, now=NOW)


def query_of(request: httpx.Request) -> dict[str, str]:
    return dict(httpx.QueryParams(request.url.query.decode()))


def page(count: int, *, start: datetime = NOW) -> dict[str, Any]:
    """Réponse `/records` synthétique de `count` enregistrements consécutifs."""
    return {
        "total_count": count,
        "results": [
            {
                "date_heure": (start + timedelta(minutes=15 * i)).isoformat(),
                "consommation": 40000 + i,
            }
            for i in range(count)
        ],
    }


# --- Fenêtre d'extraction --------------------------------------------------


def test_window_last_hours_is_utc_and_bounded() -> None:
    win = ExtractionWindow.last_hours(3, now=NOW)

    assert win.start == datetime(2026, 9, 7, 3, 0, tzinfo=UTC)
    assert win.end == NOW
    assert win.duration_hours == 3


def test_window_translates_to_odsql_with_utc_bounds(window: ExtractionWindow) -> None:
    assert window.to_odsql() == (
        "date_heure >= date'2026-09-07T03:00:00Z' AND date_heure < date'2026-09-07T06:00:00Z'"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start": NOW, "end": NOW},  # fenêtre vide
        {"start": NOW, "end": NOW - timedelta(hours=1)},  # bornes inversées
        {"start": datetime(2026, 9, 7, 3, 0), "end": NOW},  # borne naïve
    ],
)
def test_window_rejects_invalid_bounds(kwargs: dict[str, datetime]) -> None:
    with pytest.raises(ValueError):
        ExtractionWindow(**kwargs)


# --- /records --------------------------------------------------------------


def test_fetch_records_sends_expected_query(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
) -> None:
    httpx_mock.add_response(json=records_payload)

    with OdreClient(settings) as client:
        records = client.fetch_records(window)
        assert client.calls.count == 1

    assert len(records) == 2
    request = httpx_mock.get_requests()[0]
    assert request.url.path.endswith("/catalog/datasets/eco2mix-national-tr/records")
    assert query_of(request) == {
        "where": window.to_odsql(),
        "order_by": "date_heure",
        "limit": "100",
        "offset": "0",
        # Sans ce paramètre, l'interprétation du fuseau dépendrait du dataset.
        "timezone": "UTC",
    }


def test_fetch_records_paginates_until_partial_page(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(extract_odre, "RECORDS_PAGE_SIZE", 2)
    httpx_mock.add_response(json=page(2))  # page pleine -> on continue
    httpx_mock.add_response(json=page(1))  # page partielle -> on s'arrête

    with OdreClient(settings) as client:
        records = client.fetch_records(window)

        assert len(records) == 3
        assert client.calls.count == 2

    offsets = [query_of(r)["offset"] for r in httpx_mock.get_requests()]
    assert offsets == ["0", "2"]


def test_fetch_records_raises_beyond_pagination_depth(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Au-delà de la profondeur de `/records`, on échoue au lieu de perdre des lignes."""
    monkeypatch.setattr(extract_odre, "RECORDS_PAGE_SIZE", 2)
    monkeypatch.setattr(extract_odre, "RECORDS_MAX_OFFSET", 2)
    httpx_mock.add_response(json=page(2))
    httpx_mock.add_response(json=page(2))

    with (
        OdreClient(settings) as client,
        pytest.raises(PaginationLimitExceededError, match="exports/csv"),
    ):
        client.fetch_records(window)


# --- /exports/csv ----------------------------------------------------------


def test_fetch_export_csv_parses_semicolon_delimited_rows(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    export_csv: str,
) -> None:
    httpx_mock.add_response(text=export_csv)

    with OdreClient(settings) as client:
        records = client.fetch_export_csv(window)

        assert client.calls.count == 1

    assert len(records) == 2
    assert records[0]["date_heure"] == "2026-09-07T03:00:00+00:00"
    assert records[0]["consommation"] == "42000"
    assert records[1]["solaire"] == ""  # valeur manquante, typée plus tard
    request = httpx_mock.get_requests()[0]
    assert request.url.path.endswith("/exports/csv")
    assert query_of(request)["delimiter"] == ";"
    assert query_of(request)["use_labels"] == "false"


# --- Choix de l'endpoint ---------------------------------------------------


def test_fetch_window_uses_records_for_hourly_window(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
) -> None:
    """La fenêtre horaire par défaut coûte un seul appel `/records`."""
    httpx_mock.add_response(json=records_payload)

    with OdreClient(settings) as client:
        client.fetch_window(window)

        assert client.calls.count == 1

    assert httpx_mock.get_requests()[0].url.path.endswith("/records")


def test_fetch_window_switches_to_export_for_backfill(
    httpx_mock: HTTPXMock,
    settings: Settings,
    export_csv: str,
) -> None:
    """Un backfill d'un an dépasse la pagination : un seul appel `/exports/csv`."""
    backfill = ExtractionWindow(start=NOW - timedelta(days=365), end=NOW)
    assert backfill.estimated_rows() > settings.records_row_threshold
    httpx_mock.add_response(text=export_csv)

    with OdreClient(settings) as client:
        client.fetch_window(backfill)

        assert client.calls.count == 1

    assert httpx_mock.get_requests()[0].url.path.endswith("/exports/csv")


# --- Résilience ------------------------------------------------------------


def test_transient_status_is_retried(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
) -> None:
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(json=records_payload)

    with OdreClient(settings) as client:
        records = client.fetch_records(window)

        # Les deux tentatives comptent dans le quota mensuel.
        assert client.calls.count == 2

    assert len(records) == 2


def test_transport_error_is_retried(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
) -> None:
    httpx_mock.add_exception(httpx.ConnectTimeout("timeout"))
    httpx_mock.add_response(json=records_payload)

    with OdreClient(settings) as client:
        assert len(client.fetch_records(window)) == 2
        assert client.calls.count == 2


def test_client_error_is_not_retried(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
) -> None:
    """Une 400 (filtre ODSQL invalide) est définitive : la rejouer gaspillerait le quota."""
    httpx_mock.add_response(status_code=400, json={"message": "invalid where clause"})

    with OdreClient(settings) as client:
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch_records(window)

        assert client.calls.count == 1


def test_retry_gives_up_after_max_attempts(
    httpx_mock: HTTPXMock,
    window: ExtractionWindow,
) -> None:
    settings = Settings(
        _env_file=None,
        api_base_url="https://odre.test/api/explore/v2.1",
        max_retries=2,
        retry_wait_seconds=0.0,
    )
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(status_code=503)

    with OdreClient(settings) as client:
        with pytest.raises(extract_odre.RetryableStatusError):
            client.fetch_records(window)

        assert client.calls.count == 2


# --- Quota -----------------------------------------------------------------


def test_call_count_is_logged_on_close(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """La consommation du quota doit être lisible dans les logs Airflow."""
    httpx_mock.add_response(json=records_payload)
    caplog.set_level(logging.INFO, logger="ingestion.extract_odre")

    with OdreClient(settings) as client:
        client.fetch_records(window)

    assert "odre_api_calls=1" in caplog.text


def test_suite_never_opens_a_socket(
    httpx_mock: HTTPXMock,
    settings: Settings,
    window: ExtractionWindow,
    records_payload: dict[str, Any],
    no_network: None,
) -> None:
    """Garde-fou : même avec les sockets condamnés, l'extraction fonctionne."""
    httpx_mock.add_response(json=records_payload)

    with OdreClient(settings) as client:
        assert len(client.fetch_records(window)) == 2
