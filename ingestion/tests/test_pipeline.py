"""Bout-en-bout du pipeline (API simulée -> Parquet -> DuckDB) et fraîcheur."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from ingestion.config import Settings
from ingestion.pipeline import (
    StaleDataError,
    check_freshness,
    extract_to_parquet,
    run_ingestion,
)
from ingestion.warehouse import build_loader

NOW = datetime(2026, 9, 7, 6, 0, tzinfo=UTC)


@pytest.fixture
def empty_payload() -> dict[str, Any]:
    return {"total_count": 0, "results": []}


# --- Extraction ------------------------------------------------------------


def test_extract_writes_a_partitioned_parquet(
    httpx_mock: HTTPXMock,
    settings: Settings,
    records_payload: dict[str, Any],
) -> None:
    httpx_mock.add_response(json=records_payload)

    outcome = extract_to_parquet(settings=settings, run_id="manual-test", now=NOW)

    path = Path(outcome.parquet_path)
    assert path.exists()
    assert path.parent.name == "ingest_date=2026-09-07"
    assert path.parent.parent == settings.raw_dataset_dir
    assert outcome.rows == 2
    # Une fenêtre horaire coûte un seul appel : c'est la base du calcul de quota.
    assert outcome.api_calls == 1
    assert outcome.window_start == "2026-09-07T03:00:00+00:00"


def test_extract_writes_an_empty_file_when_nothing_is_published(
    httpx_mock: HTTPXMock,
    settings: Settings,
    empty_payload: dict[str, Any],
) -> None:
    """Une fenêtre vide ne doit pas faire échouer l'extraction : c'est le rôle du contrôle."""
    httpx_mock.add_response(json=empty_payload)

    outcome = extract_to_parquet(settings=settings, now=NOW)

    assert outcome.rows == 0
    assert Path(outcome.parquet_path).exists()


# --- Bout-en-bout ----------------------------------------------------------


def test_ingestion_is_idempotent_end_to_end(
    httpx_mock: HTTPXMock,
    settings: Settings,
    records_payload: dict[str, Any],
) -> None:
    """Deux runs sur la même fenêtre : les révisions écrasent, rien ne se duplique."""
    httpx_mock.add_response(json=records_payload)
    httpx_mock.add_response(json=records_payload)

    _, first = run_ingestion(settings=settings, run_id="run-1", now=NOW)
    _, second = run_ingestion(settings=settings, run_id="run-2", now=NOW + timedelta(hours=1))

    assert first.rows_inserted == 2
    assert second.rows_inserted == 0
    assert second.rows_updated == 2

    loader = build_loader(settings)
    try:
        assert loader.row_count() == 2
        assert loader.latest_timestamp() == datetime(2026, 9, 7, 3, 15, tzinfo=UTC)
    finally:
        loader.close()


# --- Fraîcheur -------------------------------------------------------------


def test_freshness_passes_within_the_threshold(
    httpx_mock: HTTPXMock,
    settings: Settings,
    records_payload: dict[str, Any],
) -> None:
    httpx_mock.add_response(json=records_payload)
    run_ingestion(settings=settings, now=NOW)

    report = check_freshness(settings=settings, now=NOW - timedelta(hours=1))

    assert report.is_fresh
    assert report.row_count == 2
    assert report.lag_hours == pytest.approx(1.75)  # 05:00 vs 03:15


def test_freshness_fails_when_data_is_late(
    httpx_mock: HTTPXMock,
    settings: Settings,
    records_payload: dict[str, Any],
) -> None:
    """Au-delà de 2 h de retard, la tâche Airflow doit échouer."""
    httpx_mock.add_response(json=records_payload)
    run_ingestion(settings=settings, now=NOW)

    with pytest.raises(StaleDataError, match="retard"):
        check_freshness(settings=settings, now=NOW + timedelta(hours=6))


def test_freshness_fails_on_an_empty_warehouse(settings: Settings) -> None:
    """Avant le premier chargement, la base n'existe même pas : échec explicite."""
    assert not settings.duckdb_path.exists()

    with pytest.raises(StaleDataError, match="aucune donnee"):
        check_freshness(settings=settings, now=NOW)
