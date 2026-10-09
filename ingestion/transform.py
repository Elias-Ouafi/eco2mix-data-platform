"""Normalisation des enregistrements ODRÉ : fuseau horaire, typage, Parquet.

Deux responsabilités, volontairement séparées du client HTTP pour rester
testables sans réseau :

1. **Fuseau horaire.** `date_heure` peut arriver soit en ISO 8601 avec décalage
   (`2026-03-29T01:45:00+01:00`), soit en heure locale naïve
   (`2026-03-29 01:45:00`) selon le typage du champ côté Opendatasoft. Les deux
   formes sont acceptées et ramenées en UTC, y compris aux changements d'heure.
2. **Typage.** Le schéma pyarrow du `DatasetSpec` fait foi : le Parquet produit a
   toujours les mêmes colonnes et les mêmes types, que l'API renvoie des entiers,
   des chaînes (cas de l'export CSV) ou des valeurs nulles.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.datasets import (
    INGESTED_AT_COLUMN,
    SOURCE_COLUMN,
    ColumnKind,
    DatasetSpec,
)

logger = logging.getLogger(__name__)

#: Fuseau de publication de RTE. Les horodatages naïfs sont exprimés dans ce fuseau.
PARIS_TZ = ZoneInfo("Europe/Paris")

_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9_.-]+")


class NonExistentLocalTimeError(ValueError):
    """L'horodatage local n'existe pas (heure sautée lors du passage à l'heure d'été)."""


def _parse_iso(raw: str) -> datetime:
    """Parse une chaîne ISO 8601, en tolérant le suffixe `Z` et l'espace séparateur."""
    text = raw.strip().replace(" ", "T")
    if text.endswith(("z", "Z")):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _is_nonexistent(naive: datetime) -> bool:
    """Vrai si l'heure locale n'existe pas (trou du passage à l'heure d'été)."""
    localized = naive.replace(tzinfo=PARIS_TZ)
    round_trip = localized.astimezone(UTC).astimezone(PARIS_TZ).replace(tzinfo=None)
    return round_trip != naive


def _is_ambiguous(naive: datetime) -> bool:
    """Vrai si l'heure locale existe deux fois (retour à l'heure d'hiver)."""
    before = naive.replace(tzinfo=PARIS_TZ, fold=0)
    after = naive.replace(tzinfo=PARIS_TZ, fold=1)
    return before.utcoffset() != after.utcoffset()


def to_utc(raw: str, previous: datetime | None = None) -> datetime:
    """Convertit un `date_heure` ODRÉ en instant UTC.

    - Chaîne avec décalage : l'instant est déjà non ambigu, on convertit.
    - Chaîne naïve : interprétée en `Europe/Paris`.
      - Heure inexistante (mars) : `NonExistentLocalTimeError`, la donnée est
        incohérente et doit être vue, pas devinée.
      - Heure ambiguë (octobre) : `previous` — le dernier instant UTC émis —
        tranche entre les deux occurrences. La première passe reste en heure
        d'été (`fold=0`), la seconde bascule en heure d'hiver (`fold=1`).
        Sans `previous`, on retient la première occurrence.

    L'appelant doit fournir les enregistrements dans l'ordre chronologique
    croissant, ce que garantit le paramètre `order_by` du client d'extraction.
    """
    parsed = _parse_iso(raw)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC)

    if _is_nonexistent(parsed):
        raise NonExistentLocalTimeError(
            f"{raw!r} n'existe pas dans le fuseau Europe/Paris (passage à l'heure d'été)"
        )

    candidate = parsed.replace(tzinfo=PARIS_TZ, fold=0).astimezone(UTC)
    if _is_ambiguous(parsed) and previous is not None and candidate <= previous:
        candidate = parsed.replace(tzinfo=PARIS_TZ, fold=1).astimezone(UTC)
    return candidate


def normalize_datetimes(raw_values: Iterable[str]) -> list[datetime]:
    """Applique `to_utc` à une série ordonnée, en propageant l'instant précédent."""
    instants: list[datetime] = []
    previous: datetime | None = None
    for raw in raw_values:
        previous = to_utc(raw, previous)
        instants.append(previous)
    return instants


def _coerce_float(value: Any, *, column: str = "") -> float | None:
    """Ramène une mesure à `float | None`.

    L'export CSV renvoie des chaînes, et ODRÉ type même certaines colonnes
    numériques en `text` (`gaz_cogen`, `ech_comm_allemagne_belgique`). Une valeur
    illisible devient `null` avec un avertissement : elle ne doit ni casser un
    chargement mensuel de dizaines de milliers de lignes, ni passer inaperçue.
    """
    if value is None:
        return None
    if isinstance(value, bool):  # garde-fou : un booléen n'est pas une mesure
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        logger.warning("valeur non numerique ignoree colonne=%s valeur=%r", column, text)
        return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_instant_column(
    records: Sequence[dict[str, Any]],
    *,
    name: str,
    group_names: Sequence[str],
) -> list[datetime]:
    """Normalise une colonne d'instant en UTC, groupe par groupe.

    La lève d'ambiguïté d'octobre s'appuie sur l'instant précédent de la série.
    Or un dataset régional publie le même instant pour les douze régions : pris
    à plat, le deuxième enregistrement d'un instant répété serait vu comme un
    retour en arrière et basculerait à tort en heure d'hiver. On suit donc un
    instant précédent **par groupe** (les autres colonnes de la clé).
    """
    previous: dict[tuple[str | None, ...], datetime] = {}
    instants: list[datetime] = []
    for record in records:
        group = tuple(_coerce_str(record.get(g)) for g in group_names)
        value = to_utc(str(record[name]), previous.get(group))
        previous[group] = value
        instants.append(value)
    return instants


def records_to_table(
    records: Sequence[dict[str, Any]],
    *,
    spec: DatasetSpec,
    ingested_at: datetime,
) -> pa.Table:
    """Construit la table pyarrow typée à partir des enregistrements bruts.

    Les enregistrements dont une colonne de clé est absente sont écartés (avec un
    avertissement) : ils ne peuvent pas participer au MERGE. Les colonnes du spec
    absentes de la réponse sont créées à `null` et signalées — c'est le garde-fou
    contre un renommage de champ côté ODRÉ. Les colonnes renvoyées par l'API mais
    absentes du spec sont ignorées.
    """
    if ingested_at.tzinfo is None:
        raise ValueError("ingested_at doit être un datetime aware")
    ingested_at_utc = ingested_at.astimezone(UTC)

    key_names = spec.key_names
    usable = [r for r in records if all(_coerce_str(r.get(n)) for n in key_names)]
    if (dropped := len(records) - len(usable)) > 0:
        logger.warning(
            "%d enregistrement(s) sans cle complete %s ignore(s)", dropped, list(key_names)
        )

    if usable:
        seen: set[str] = set().union(*(set(r.keys()) for r in usable))
        technical = {INGESTED_AT_COLUMN, SOURCE_COLUMN}
        if missing := sorted(set(spec.schema.names) - seen - technical):
            logger.warning("colonnes absentes de la reponse API, remplies a null : %s", missing)

    group_names = [n for n in key_names if n != spec.instant_column]
    columns: dict[str, list[Any]] = {}
    for column in spec.columns:
        if column.kind is ColumnKind.INSTANT:
            columns[column.name] = _normalize_instant_column(
                usable, name=column.name, group_names=group_names
            )
        elif column.kind is ColumnKind.MEASURE:
            columns[column.name] = [
                _coerce_float(r.get(column.name), column=column.name) for r in usable
            ]
        else:
            columns[column.name] = [_coerce_str(r.get(column.name)) for r in usable]
    columns[INGESTED_AT_COLUMN] = [ingested_at_utc] * len(usable)
    columns[SOURCE_COLUMN] = [spec.dataset_id] * len(usable)

    table = pa.table(columns, schema=spec.schema)
    return table.sort_by([(name, "ascending") for name in key_names])


def _safe_run_id(run_id: str) -> str:
    """Neutralise les caractères interdits dans un nom de fichier Windows/Linux.

    Les `run_id` Airflow contiennent `:` et `+` (`scheduled__2026-09-07T05:00:00+00:00`).
    """
    return _UNSAFE_FILENAME_CHARS.sub("-", run_id).strip("-") or "run"


def write_parquet(
    table: pa.Table,
    *,
    dataset_dir: Path,
    run_id: str,
    ingest_date: date,
) -> Path:
    """Écrit la table dans `<dataset_dir>/ingest_date=YYYY-MM-DD/part-<run_id>.parquet`."""
    partition = dataset_dir / f"ingest_date={ingest_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"part-{_safe_run_id(run_id)}.parquet"
    pq.write_table(table, path, compression="zstd")
    logger.info("parquet ecrit rows=%d path=%s", table.num_rows, path)
    return path
