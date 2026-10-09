"""Géocodage d'adresse : résolution du territoire et robustesse.

Aucun appel réseau réel : `pytest-httpx` intercepte au niveau du transport.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from ingestion.config import Settings
from ingestion.geocode import (
    REGIONS,
    AdresseIntrouvableError,
    BanClient,
    GeocodingError,
    code_region,
    geocode,
    normalise,
)

BAN_URL = "https://ban.test/geocodage"


@pytest.fixture
def ban_settings(settings: Settings) -> Settings:
    return settings.model_copy(update={"ban_base_url": BAN_URL})


def feature(**props: Any) -> dict[str, Any]:
    """Réponse BAN minimale, surchargeable champ par champ."""
    defaults = {
        "label": "12 Rue de la Paix 69003 Lyon",
        "score": 0.96,
        "type": "housenumber",
        "citycode": "69383",
        "city": "Lyon",
        "postcode": "69003",
        "context": "69, Rhône, Auvergne-Rhône-Alpes",
    }
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.8456, 45.7578]},
                "properties": defaults | props,
            }
        ],
    }


# --- Normalisation des noms de région --------------------------------------


def test_region_names_match_whatever_the_spelling() -> None:
    """BAN et ODRÉ n'écrivent pas les régions de la même façon."""
    assert code_region("Auvergne-Rhône-Alpes") == "84"
    assert code_region("AUVERGNE-RHONE-ALPES") == "84"
    assert code_region("Provence-Alpes-Côte d'Azur") == "93"
    assert code_region("PROVENCE-ALPES-COTE D'AZUR") == "93"
    assert code_region("Île-de-France") == "11"
    assert code_region("ILE DE FRANCE") == "11"
    assert code_region("Pays de la Loire") == "52"


def test_unknown_region_returns_none() -> None:
    assert code_region("Bourgogne") is None
    assert code_region("") is None


def test_normalise_strips_accents_case_and_punctuation() -> None:
    assert normalise("Côte d'Azur") == normalise("COTE D AZUR")


def test_every_region_code_is_unique_and_two_digits() -> None:
    assert len(REGIONS) == 18
    assert all(len(c) == 2 and c.isdigit() for c in REGIONS)
    assert len(set(REGIONS.values())) == len(REGIONS)


# --- Résolution nominale ---------------------------------------------------


def test_address_resolves_to_its_territory(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    httpx_mock.add_response(json=feature())

    territoire = geocode("12 rue de la Paix, Lyon", settings=ban_settings)

    assert territoire.code_insee_commune == "69383"
    assert territoire.commune == "Lyon"
    assert territoire.code_postal == "69003"
    assert territoire.code_departement == "69"
    assert territoire.departement == "Rhône"
    assert territoire.region == "Auvergne-Rhône-Alpes"
    # La clé de jointure avec les jeux ODRÉ, absente de la réponse BAN.
    assert territoire.code_insee_region == "84"
    assert (territoire.latitude, territoire.longitude) == (45.7578, 4.8456)
    assert territoire.est_precis


def test_query_is_sent_with_the_address_and_a_single_result(
    httpx_mock: HTTPXMock, ban_settings: Settings
) -> None:
    httpx_mock.add_response(json=feature())

    geocode("12 rue de la Paix, Lyon", settings=ban_settings)

    request = httpx_mock.get_requests()[0]
    assert str(request.url).startswith(f"{BAN_URL}/search")
    assert request.url.params["q"] == "12 rue de la Paix, Lyon"
    assert request.url.params["limit"] == "1"
    assert request.url.params["index"] == "address"


def test_overseas_context_with_two_segments(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    """Outre-mer, département et région portent le même nom."""
    httpx_mock.add_response(
        json=feature(citycode="97411", city="Saint-Denis", context="974, La Réunion")
    )

    territoire = geocode("Saint-Denis", settings=ban_settings)

    assert territoire.code_departement == "974"
    assert territoire.region == "La Réunion"
    assert territoire.code_insee_region == "04"


def test_corsica_department_codes_are_kept_as_text(
    httpx_mock: HTTPXMock, ban_settings: Settings
) -> None:
    """2A n'est pas un nombre : un code département reste du texte."""
    httpx_mock.add_response(
        json=feature(citycode="2A004", city="Ajaccio", context="2A, Corse-du-Sud, Corse")
    )

    territoire = geocode("Ajaccio", settings=ban_settings)

    assert territoire.code_departement == "2A"
    assert territoire.code_insee_region == "94"


# --- Cas dégradés ----------------------------------------------------------


def test_no_result_raises(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    httpx_mock.add_response(json={"type": "FeatureCollection", "features": []})

    with pytest.raises(AdresseIntrouvableError, match="aucun resultat"):
        geocode("zzzz", settings=ban_settings)


def test_empty_address_never_calls_the_api(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    """Inutile de consommer un appel pour une chaîne vide."""
    with pytest.raises(AdresseIntrouvableError, match="adresse vide"):
        geocode("   ", settings=ban_settings)

    assert httpx_mock.get_requests() == []


def test_low_score_is_warned_but_returned(
    httpx_mock: HTTPXMock,
    ban_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Un score faible reste exploitable : c'est à l'appelant de juger."""
    httpx_mock.add_response(json=feature(score=0.21))

    with caplog.at_level("WARNING"):
        territoire = geocode("rue qui n'existe pas", settings=ban_settings)

    assert territoire.score == 0.21
    assert "score de geocodage faible" in caplog.text


def test_commune_level_match_is_flagged(
    httpx_mock: HTTPXMock,
    ban_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    httpx_mock.add_response(json=feature(type="municipality", label="Lyon"))

    with caplog.at_level("WARNING"):
        territoire = geocode("Lyon", settings=ban_settings)

    assert not territoire.est_precis
    assert "maille" in caplog.text


def test_unknown_region_is_warned_and_code_left_empty(
    httpx_mock: HTTPXMock,
    ban_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Mieux vaut un code absent qu'un code inventé : la jointure échouera visiblement."""
    httpx_mock.add_response(json=feature(context="99, Ailleurs, Région Inconnue"))

    with caplog.at_level("WARNING"):
        territoire = geocode("ailleurs", settings=ban_settings)

    assert territoire.code_insee_region is None
    assert "non reconnue" in caplog.text


def test_response_without_citycode_raises(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    httpx_mock.add_response(json=feature(citycode=None))

    with pytest.raises(GeocodingError, match="sans code commune"):
        geocode("quelque part", settings=ban_settings)


def test_malformed_context_raises(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    httpx_mock.add_response(json=feature(context="69"))

    with pytest.raises(GeocodingError, match="context inattendu"):
        geocode("quelque part", settings=ban_settings)


# --- Robustesse réseau -----------------------------------------------------


def test_transient_status_is_retried(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(json=feature())

    with BanClient(ban_settings) as client:
        territoire = client.geocode("12 rue de la Paix, Lyon")

        assert territoire.code_insee_commune == "69383"
        assert client.calls == 2


def test_client_error_is_not_retried(httpx_mock: HTTPXMock, ban_settings: Settings) -> None:
    """Une 400 ne deviendra pas une 200 au deuxième essai."""
    httpx_mock.add_response(status_code=400)

    with BanClient(ban_settings) as client:
        with pytest.raises(httpx.HTTPStatusError):
            client.geocode("12 rue de la Paix, Lyon")

        assert client.calls == 1
