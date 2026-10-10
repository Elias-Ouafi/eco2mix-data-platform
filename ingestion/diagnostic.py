"""Diagnostic d'implantation pour une adresse géocodée.

Lit la couche gold en lecture seule et ne recalcule aucune règle métier : la
pression industrielle, la tension régionale, les seuils de raccordement et les
limites méthodologiques sont tous construits par dbt. Ce module ne fait que
sélectionner, assembler et mettre en forme.

Points de conception :

* **Le département, maille garantie.** `gold.fct_diagnostic_territoire` a une
  ligne par département ; la commune s'y ajoute par jointure quand elle figure
  dans `gold.fct_pression_industrielle_commune` (environ 1 300 communes sur
  35 000). Sinon, le diagnostic le dit et s'en tient au département.
* **Arrondissements.** La BAN renvoie le code de l'arrondissement (69383 pour
  Lyon 3e) ; ODRÉ publie Paris, Lyon et Marseille sous le code de la commune.
* **Aucun chiffre sans sa limite.** Chaque bloc de la sortie texte est suivi de
  la limite correspondante, lue dans `gold.limites_methodologiques`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from ingestion.config import Settings, get_settings
from ingestion.geocode import Territoire

#: Arrondissements municipaux (3 premiers caractères du code) -> code de la commune.
ARRONDISSEMENTS: dict[str, str] = {"751": "75056", "693": "69123", "132": "13055"}

_DIAGNOSTIC_SQL = """
select
    territoire.*,
    commune.commune,
    commune.consommation_electricite_mwh / 1000 as conso_industrielle_commune_gwh,
    commune.nb_sites_electricite as nb_sites_commune,
    commune.est_minorant as conso_commune_est_minorant
from gold.fct_diagnostic_territoire as territoire
left join gold.fct_pression_industrielle_commune as commune
    on commune.code_insee_commune = $commune
where territoire.code_departement = $departement
"""

_SEUIL_SQL = """
select domaine_tension, gestionnaire, commentaire
from gold.seuils_raccordement
where $puissance >= puissance_min_mw
  and (puissance_max_mw is null or $puissance < puissance_max_mw)
"""


class DiagnosticIndisponibleError(RuntimeError):
    """Le warehouse ne permet pas de produire le diagnostic."""


def code_commune_parent(code_insee: str) -> str:
    """Ramène un arrondissement de Paris, Lyon ou Marseille au code de sa commune."""
    if len(code_insee) == 5 and code_insee[:3] in ARRONDISSEMENTS:
        return ARRONDISSEMENTS[code_insee[:3]]
    return code_insee


def code_departement(code_commune: str) -> str:
    """Code département d'un code commune : 3 caractères outre-mer, 2 ailleurs.

    Pendant Python de la macro dbt `code_departement`.
    """
    return code_commune[:3] if code_commune.startswith("97") else code_commune[:2]


@dataclass(frozen=True, slots=True)
class Seuil:
    """Domaine de tension et gestionnaire applicables à une puissance."""

    puissance_mw: float
    domaine_tension: str
    gestionnaire: str
    commentaire: str


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """Diagnostic complet d'une adresse : une ligne de gold, plus la commune."""

    territoire: Territoire
    code_insee_commune: str
    # Territoire
    code_departement: str
    departement: str
    code_insee_region: str
    region: str
    est_zni: bool
    # Pression industrielle
    annee_pression: int | None
    commune: str | None
    conso_industrielle_commune_gwh: float | None
    nb_sites_commune: int | None
    conso_commune_est_minorant: bool | None
    conso_industrielle_departement_gwh: float
    nb_sites_departement: int
    nb_communes_avec_site: int
    conso_departement_est_minorant: bool
    rang_pression_departement: int | None
    nb_departements_classes: int
    # Tension du réseau régional
    equilibre_mois_debut: date | None
    equilibre_mois_fin: date | None
    equilibre_nb_mois: int
    production_region_gwh: float | None
    consommation_region_gwh: float | None
    solde_region_gwh: float | None
    taux_couverture_region: float | None
    statut_equilibre: str | None
    rang_dependance_region: int | None
    nb_regions_classees: int
    puissance_enr_installee_mw: float | None
    puissance_a_compenser_mw: float | None
    energie_non_evacuee_mwh: float | None
    # Raccordement et limites
    seuil: Seuil | None
    limites: dict[str, str]

    @property
    def commune_a_des_sites(self) -> bool:
        return bool(self.nb_sites_commune)


def lire_diagnostic(
    territoire: Territoire,
    *,
    puissance_mw: float | None = None,
    settings: Settings | None = None,
    database: Path | None = None,
) -> Diagnostic:
    """Assemble le diagnostic d'un territoire depuis la couche gold.

    Lève `DiagnosticIndisponibleError` si le warehouse est absent ou si la
    couche gold n'a pas été construite (`dbt build`).
    """
    path = database or (settings or get_settings()).duckdb_path
    if not Path(path).exists():
        raise DiagnosticIndisponibleError(
            f"warehouse introuvable : {path} (charger les donnees puis lancer dbt build)"
        )

    commune = code_commune_parent(territoire.code_insee_commune)
    departement = code_departement(commune)
    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            ligne = _une_ligne(
                connection, _DIAGNOSTIC_SQL, {"commune": commune, "departement": departement}
            )
            seuil = _seuil(connection, puissance_mw) if puissance_mw is not None else None
            limites = dict(
                connection.execute(
                    "select indicateur, limite from gold.limites_methodologiques"
                ).fetchall()
            )
    except duckdb.CatalogException as error:
        raise DiagnosticIndisponibleError(
            f"couche gold incomplete, lancer dbt build ({error})"
        ) from error

    if ligne is None:
        raise DiagnosticIndisponibleError(
            f"departement {departement!r} absent de gold.fct_diagnostic_territoire"
        )
    return Diagnostic(
        territoire=territoire,
        code_insee_commune=commune,
        seuil=seuil,
        limites=limites,
        **ligne,
    )


def _une_ligne(
    connection: duckdb.DuckDBPyConnection, sql: str, parametres: dict[str, Any]
) -> dict[str, Any] | None:
    result = connection.execute(sql, parametres)
    row = result.fetchone()
    if row is None:
        return None
    return dict(zip([d[0] for d in result.description], row, strict=True))


def _seuil(connection: duckdb.DuckDBPyConnection, puissance_mw: float) -> Seuil | None:
    if puissance_mw < 0:
        raise ValueError("la puissance du projet doit etre positive")
    ligne = _une_ligne(connection, _SEUIL_SQL, {"puissance": puissance_mw})
    return None if ligne is None else Seuil(puissance_mw=puissance_mw, **ligne)


# ---------------------------------------------------------------------------
# Sortie texte
# ---------------------------------------------------------------------------


def _nombre(valeur: float, decimales: int = 0) -> str:
    return f"{valeur:,.{decimales}f}".replace(",", " ").replace(".", ",")


def _gwh(valeur: float) -> str:
    return f"{_nombre(valeur, 1 if valeur < 100 else 0)} GWh"


def formater_diagnostic(diagnostic: Diagnostic) -> str:
    """Diagnostic lisible en terminal, chaque bloc suivi de sa limite."""
    d = diagnostic
    t = d.territoire
    lignes = [
        f"DIAGNOSTIC D'IMPLANTATION — {t.adresse}",
        f"Commune {t.commune} ({d.code_insee_commune}) · {d.departement} ({d.code_departement})"
        f" · {d.region} ({d.code_insee_region})",
    ]
    if not t.est_precis:
        lignes.append(f"Attention : adresse résolue à la maille « {t.precision} » seulement.")

    def limite(indicateur: str) -> None:
        if texte := d.limites.get(indicateur):
            lignes.append(f"   ↳ Limite : {texte}")

    # 1. Pression industrielle
    lignes += [
        "",
        "1. Pression industrielle locale"
        f" (sites raccordés au réseau de transport, {d.annee_pression or '?'})",
    ]
    if d.commune_a_des_sites:
        minorant = " (minorant : secret statistique)" if d.conso_commune_est_minorant else ""
        lignes.append(
            f"   Commune     : {d.nb_sites_commune} site(s),"
            f" {_gwh(d.conso_industrielle_commune_gwh or 0)}/an{minorant}"
        )
    else:
        lignes.append(
            "   Commune     : aucun site raccordé au transport → repli sur le département"
        )
    if d.nb_sites_departement:
        rang = (
            f" — rang {d.rang_pression_departement} sur {d.nb_departements_classes} départements"
            if d.rang_pression_departement
            else ""
        )
        lignes.append(
            f"   Département : {d.nb_sites_departement} site(s) dans {d.nb_communes_avec_site}"
            f" commune(s), {_gwh(d.conso_industrielle_departement_gwh)}/an{rang}"
        )
    else:
        lignes.append("   Département : aucun site raccordé au transport")
    limite("pression_industrielle")

    # 2. Tension du réseau régional
    lignes.append("")
    if d.taux_couverture_region is None:
        zni = " (zone non interconnectée)" if d.est_zni else ""
        lignes += [
            "2. Tension du réseau régional",
            f"   Aucun équilibre régional publié par RTE pour cette région{zni}.",
        ]
    else:
        debut = f"{d.equilibre_mois_debut:%m/%Y}" if d.equilibre_mois_debut else "?"
        fin = f"{d.equilibre_mois_fin:%m/%Y}" if d.equilibre_mois_fin else "?"
        lignes += [
            f"2. Tension du réseau régional ({debut} – {fin}, {d.equilibre_nb_mois} mois)",
            f"   Production {_gwh(d.production_region_gwh or 0)},"
            f" consommation {_gwh(d.consommation_region_gwh or 0)}"
            f" : couverture {_nombre(d.taux_couverture_region * 100)} %"
            f" → région {_STATUTS.get(d.statut_equilibre or '', '?')}",
            f"   Rang de dépendance : {d.rang_dependance_region} sur {d.nb_regions_classees}"
            " (1 = la plus déficitaire)",
        ]
        if d.est_zni:
            lignes.append(
                "   Zone non interconnectée : le réseau n'est relié au continent que par des"
                " liaisons limitées, le déficit y pèse bien davantage qu'en métropole."
            )
    limite("equilibre_regional")
    if d.puissance_a_compenser_mw is not None:
        lignes.append(
            f"   Contraintes d'évacuation (RTE) : {_nombre(d.puissance_enr_installee_mw or 0)} MW"
            f" d'EnR installés ou en projet, {_nombre(d.puissance_a_compenser_mw)} MW à"
            f" compenser, {_nombre(d.energie_non_evacuee_mwh or 0)} MWh non évacués par an"
        )
        limite("contraintes_reseau")

    # 3. Raccordement
    lignes += ["", "3. Raccordement"]
    if d.seuil is not None:
        s = d.seuil
        lignes.append(
            f"   Puissance {_nombre_court(s.puissance_mw)} MW → {s.domaine_tension},"
            f" {s.gestionnaire} : {s.commentaire}"
        )
        limite("seuil_raccordement")
    else:
        lignes.append("   Préciser --puissance (en MW) pour connaître le domaine de tension.")
    limite("capacite_raccordement")
    return "\n".join(lignes)


_STATUTS = {"excedentaire": "excédentaire", "deficitaire": "déficitaire"}


def _nombre_court(valeur: float) -> str:
    """« 0,5 », « 12 » : décimales utiles seulement."""
    return _nombre(valeur, 3).rstrip("0").rstrip(",")
