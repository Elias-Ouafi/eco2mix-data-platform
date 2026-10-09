"""Géocodage d'une adresse française vers son territoire administratif.

C'est le point d'entrée du diagnostic d'implantation : l'adresse passée en
argument de la ligne de commande est résolue en codes administratifs, qui servent
ensuite de clés de jointure avec les jeux territoriaux d'ODRÉ.

Points de conception :

* **API Adresse de la Géoplateforme (IGN).** L'ancien point d'accès
  `api-adresse.data.gouv.fr` a été décommissionné fin janvier 2026 ; le service
  vit désormais sur `https://data.geopf.fr/geocodage`. Limite d'usage : 50
  requêtes par seconde et par adresse IP.
* **Pas d'IRIS.** La BAN ne renvoie pas le code IRIS, et l'obtenir supposerait une
  jointure spatiale avec les contours INSEE — donc une dépendance géospatiale.
  Inutile ici : le jeu `consommation-annuelle-par-iris` porte déjà
  `code_insee_commune`, qui suffit à agréger à la commune.
* **Code région reconstitué.** La BAN renvoie le *nom* de la région, les jeux
  ODRÉ sa clé INSEE. La correspondance est faite localement, sur un nom
  normalisé (sans accent ni casse), parce que les deux sources n'écrivent pas
  « Provence-Alpes-Côte d'Azur » de la même façon.
* **Même robustesse que les clients ODRÉ et RTE.** Timeout explicite, retry
  exponentiel sur erreurs réseau et statuts transitoires, jamais sur une 4xx.
"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import Settings, get_settings
from ingestion.extract_odre import RETRYABLE_STATUS_CODES, USER_AGENT, RetryableStatusError

logger = logging.getLogger(__name__)

#: En dessous, la BAN n'a pas trouvé grand-chose de ressemblant.
SCORE_MINIMUM = 0.4

#: Régions françaises : code INSEE -> nom de référence.
#: Donnée de référence stable (dernière réforme : 2016), volontairement figée ici
#: plutôt qu'appelée à chaque diagnostic.
REGIONS: dict[str, str] = {
    "84": "Auvergne-Rhône-Alpes",
    "27": "Bourgogne-Franche-Comté",
    "53": "Bretagne",
    "24": "Centre-Val de Loire",
    "94": "Corse",
    "44": "Grand Est",
    "32": "Hauts-de-France",
    "11": "Île-de-France",
    "28": "Normandie",
    "75": "Nouvelle-Aquitaine",
    "76": "Occitanie",
    "52": "Pays de la Loire",
    "93": "Provence-Alpes-Côte d'Azur",
    "01": "Guadeloupe",
    "02": "Martinique",
    "03": "Guyane",
    "04": "La Réunion",
    "06": "Mayotte",
}


class GeocodingError(RuntimeError):
    """Le géocodage n'a pas abouti."""


class AdresseIntrouvableError(GeocodingError):
    """Aucun résultat exploitable pour l'adresse demandée."""


def normalise(valeur: str) -> str:
    """Normalise un nom pour comparaison : sans accent, sans casse, sans ponctuation.

    « Provence-Alpes-Côte d'Azur », « PROVENCE-ALPES-COTE D'AZUR » et
    « Provence Alpes Cote dAzur » doivent se rejoindre.
    """
    sans_accent = unicodedata.normalize("NFKD", valeur)
    sans_accent = "".join(c for c in sans_accent if not unicodedata.combining(c))
    return "".join(c for c in sans_accent.lower() if c.isalnum())


_REGIONS_PAR_NOM: dict[str, str] = {normalise(nom): code for code, nom in REGIONS.items()}


def code_region(nom: str) -> str | None:
    """Code INSEE d'une région à partir de son nom, quelle qu'en soit l'écriture."""
    return _REGIONS_PAR_NOM.get(normalise(nom))


@dataclass(frozen=True, slots=True)
class Territoire:
    """Territoire administratif résolu, clés de jointure comprises."""

    adresse: str
    code_insee_commune: str
    commune: str
    code_postal: str | None
    code_departement: str
    departement: str
    code_insee_region: str | None
    region: str
    latitude: float
    longitude: float
    score: float
    #: Précision du résultat BAN : `housenumber`, `street`, `locality`, `municipality`.
    precision: str

    @property
    def est_precis(self) -> bool:
        """Vrai si la BAN a résolu une voie ou un numéro, pas seulement une commune."""
        return self.precision in {"housenumber", "street"}

    def __str__(self) -> str:
        return (
            f"{self.adresse} [commune {self.code_insee_commune}, "
            f"dept {self.code_departement}, region {self.code_insee_region or '?'}]"
        )


def _contexte(valeur: str) -> tuple[str, str, str]:
    """Découpe le champ `context` de la BAN : « 69, Rhône, Auvergne-Rhône-Alpes ».

    Certaines réponses n'ont que deux segments (collectivités d'outre-mer, où le
    département et la région portent le même nom) : la région reprend alors le
    nom du département.
    """
    parts = [p.strip() for p in valeur.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return parts[0], parts[1], parts[1]
    raise GeocodingError(f"champ context inattendu : {valeur!r}")


class BanClient:
    """Client de l'API Adresse (Géoplateforme), utilisable comme context manager."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.calls = 0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=self.settings.ban_base_url,
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
        logger.info("ban_api_calls=%d", self.calls)
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> BanClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- Géocodage ----------------------------------------------------------

    def geocode(self, adresse: str) -> Territoire:
        """Résout une adresse en territoire administratif.

        Lève `AdresseIntrouvableError` si la BAN ne renvoie rien. Un score faible
        ou une résolution limitée à la commune passent, mais sont journalisés :
        c'est à l'appelant de décider si le diagnostic reste pertinent.
        """
        if not adresse.strip():
            raise AdresseIntrouvableError("adresse vide")

        payload = self._get(
            "search",
            {"q": adresse, "index": "address", "limit": 1, "returntruegeometry": "false"},
        ).json()
        features = payload.get("features") or []
        if not features:
            raise AdresseIntrouvableError(f"aucun resultat pour {adresse!r}")

        return self._to_territoire(features[0], demande=adresse)

    def _to_territoire(self, feature: dict[str, Any], *, demande: str) -> Territoire:
        props: dict[str, Any] = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None]

        citycode = props.get("citycode")
        if not citycode:
            raise GeocodingError(f"reponse BAN sans code commune pour {demande!r}")

        code_dept, departement, region = _contexte(str(props.get("context", "")))
        score = float(props.get("score") or 0.0)
        precision = str(props.get("type") or "")

        if score < SCORE_MINIMUM:
            logger.warning(
                "score de geocodage faible (%.2f) pour %r : resultat retenu %r",
                score,
                demande,
                props.get("label"),
            )
        if precision not in {"housenumber", "street"}:
            logger.warning(
                "adresse resolue a la maille %r seulement pour %r", precision or "inconnue", demande
            )

        insee_region = code_region(region)
        if insee_region is None:
            logger.warning("region %r non reconnue : code INSEE absent du diagnostic", region)

        return Territoire(
            adresse=str(props.get("label") or demande),
            code_insee_commune=str(citycode),
            commune=str(props.get("city") or ""),
            code_postal=str(props["postcode"]) if props.get("postcode") else None,
            code_departement=code_dept,
            departement=departement,
            code_insee_region=insee_region,
            region=region,
            latitude=float(coords[1]) if coords[1] is not None else 0.0,
            longitude=float(coords[0]) if coords[0] is not None else 0.0,
            score=score,
            precision=precision,
        )

    # -- Couche HTTP --------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any]) -> httpx.Response:
        """GET avec retry exponentiel sur les erreurs transitoires."""

        def envoyer() -> httpx.Response:
            self.calls += 1
            response = self._client.get(path, params=params)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableStatusError(response.status_code)
            response.raise_for_status()
            return response

        return self._retrying(envoyer)


def geocode(adresse: str, *, settings: Settings | None = None) -> Territoire:
    """Géocode une adresse en ouvrant et fermant un client dédié."""
    with BanClient(settings) as client:
        return client.geocode(adresse)
