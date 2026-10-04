"""Version Markdown du rapport mensuel : mêmes chiffres que le PDF, en texte brut.

Le document est lisible tel quel (GitHub, VS Code, Obsidian…) et se versionne
ou se colle facilement dans un autre outil. Les graphiques, optionnels, sont
écrits en PNG dans un dossier voisin `<nom>_graphiques/` et référencés par des
liens relatifs : le dossier se déplace d'un bloc avec le document.
"""

from __future__ import annotations

from pathlib import Path

from reporting.data import Creneau, RapportMensuel
from reporting.formats import (
    co2,
    creneau_tarif,
    euros,
    heure,
    jour,
    nom_mois,
    nombre,
    plage,
    prix,
)


def _tableau(entetes: list[str], lignes: list[list[str]]) -> str:
    def _ligne(cellules: list[str]) -> str:
        # Une barre verticale dans une cellule casserait le tableau.
        return "| " + " | ".join(str(c).replace("|", "\\|") for c in cellules) + " |"

    return "\n".join(
        [_ligne(entetes), "|" + "|".join("---" for _ in entetes) + "|"]
        + [_ligne(ligne) for ligne in lignes]
    )


class _Graphiques:
    """Écrit les graphiques à côté du document et renvoie leur lien Markdown relatif."""

    def __init__(self, document: Path, rapport: RapportMensuel, actif: bool) -> None:
        self.rapport = rapport
        self.actif = actif
        self.dossier = document.with_name(f"{document.stem}_graphiques")

    def image(self, nom: str, legende: str) -> list[str]:
        """`nom` est une fonction de `reporting.charts`, rendue en `<nom>.png`."""
        if not self.actif:
            return []
        # Import paresseux : sans graphiques, matplotlib n'est pas nécessaire.
        from reporting import charts

        self.dossier.mkdir(parents=True, exist_ok=True)
        (self.dossier / f"{nom}.png").write_bytes(getattr(charts, nom)(self.rapport))
        return [f"![{legende}]({self.dossier.name}/{nom}.png)", ""]


def _avertissements(rapport: RapportMensuel) -> list[str]:
    messages = []
    if rapport.couverture < 0.98:
        messages.append(
            f"Données éCO2mix incomplètes : {rapport.heures_completes} heures mesurées sur "
            f"{rapport.heures_attendues} ({rapport.couverture:.0%}). Les totaux portent sur les "
            "heures disponibles."
        )
    if rapport.jours_tempo_connus < rapport.jours_du_mois:
        manquants = rapport.jours_du_mois - rapport.jours_tempo_connus
        suite = (
            "Les prix ne sont pas disponibles : les créneaux sont classés sur le seul critère "
            "carbone."
            if not rapport.prix_disponibles
            else "Les prix de ces jours ne sont pas pris en compte."
        )
        messages.append(f"Calendrier Tempo incomplet : {manquants} jour(s) sans couleur. {suite}")
    elif rapport.a_des_donnees and not rapport.prix_disponibles:
        messages.append(
            "Aucun tarif Tempo connu pour cette période : les créneaux sont classés sur le seul "
            "critère carbone."
        )
    return [f"> **À noter.** {m}\n" for m in messages]


def _en_bref(rapport: RapportMensuel) -> list[str]:
    propre, carbonee = rapport.heure_plus_propre, rapport.heure_plus_carbonee
    lignes = [
        ["Intensité CO₂ moyenne du mois", co2(rapport.co2_moyen)],
        [
            "Heure la plus propre",
            f"{co2(propre.taux_co2)} — {jour(propre.heure_paris.date())}, "
            f"{heure(propre.heure_paris)}",
        ],
        [
            "Pic maximal",
            f"{co2(carbonee.taux_co2)} — {jour(carbonee.heure_paris.date())}, "
            f"{heure(carbonee.heure_paris)}",
        ],
        [
            "Jours rouges / blancs / bleus",
            " / ".join(str(rapport.repartition_tempo.get(c, 0)) for c in ("rouge", "blanc", "bleu"))
            if rapport.jours_tempo_connus
            else "n.d.",
        ],
    ]
    if rapport.meilleurs_creneaux:
        meilleur = rapport.meilleurs_creneaux[0]
        valeur = (
            prix(meilleur.prix_moyen)
            if meilleur.prix_moyen is not None
            else co2(meilleur.co2_moyen)
        )
        lignes.append(["Meilleur créneau", f"{plage(meilleur.debut, meilleur.fin)} ({valeur})"])
    if rapport.economie is not None:
        e = rapport.economie
        evites = f"{nombre(e.gain_co2_kg, 1)} kg de CO₂ évités"
        valeur = evites if e.gain_euros is None else f"{euros(e.gain_euros)} et {evites}"
        lignes.append([f"Économie en déplaçant {nombre(e.kwh_par_jour)} kWh/jour", valeur])
    return ["## En bref", "", _tableau(["Indicateur", "Valeur"], lignes), ""]


def _carbone(rapport: RapportMensuel, graphiques: _Graphiques) -> list[str]:
    blocs = [
        "## 1. Pics de carbone",
        "",
        "L'intensité CO₂ mesure les émissions de la production électrique française, heure par "
        "heure. Une heure est un **pic** lorsqu'elle dépasse le 9e décile du mois — "
        f"**{co2(rapport.seuil_pic)}** ce mois-ci. Les pics surviennent lorsque les centrales "
        "thermiques (gaz, charbon, fioul) sont le plus sollicitées, souvent en soirée.",
        "",
        *graphiques.image("courbe_carbone", "Intensité carbone heure par heure"),
    ]
    if rapport.episodes:
        blocs += [
            "### Épisodes de pic les plus intenses",
            "",
            _tableau(
                ["Période", "Durée", "CO₂ max", "CO₂ moyen"],
                [
                    [plage(e.debut, e.fin), f"{e.duree_heures} h", co2(e.co2_max), co2(e.co2_moyen)]
                    for e in rapport.episodes
                ],
            ),
            "",
        ]
    if rapport.jours_plus_carbones:
        blocs += [
            "### Jours les plus carbonés",
            "",
            _tableau(
                ["Jour", "CO₂ moyen", "Couleur Tempo"],
                [
                    [jour(j), co2(v), rapport.calendrier.get(j, "inconnue")]
                    for j, v in rapport.jours_plus_carbones
                ],
            ),
            "",
        ]
    blocs += graphiques.image("carte_chaleur", "Intensité carbone par jour et par heure")
    return blocs


def _tempo(rapport: RapportMensuel, graphiques: _Graphiques) -> list[str]:
    blocs = [
        "## 2. Calendrier Tempo et prix",
        "",
        "Avec l'option Tempo, le prix du kWh dépend de la couleur du jour, fixée la veille par "
        "RTE, et de l'heure : heures creuses de 22 h à 6 h, heures pleines de 6 h à 22 h. Un jour "
        "Tempo court de 6 h à 6 h le lendemain.",
        "",
    ]
    if not rapport.jours_tempo_connus:
        blocs += [
            "Le calendrier Tempo de ce mois n'est pas chargé "
            "(`python -m ingestion.cli tempo --month AAAA-MM`, identifiants RTE requis).",
            "",
        ]
        return blocs
    blocs += graphiques.image("calendrier_tempo", "Calendrier Tempo du mois")
    if rapport.jours_rouges:
        blocs += [
            "**Jours rouges :** " + ", ".join(jour(j) for j in rapport.jours_rouges) + ".",
            "",
        ]
    if rapport.synthese:
        blocs += [
            "### Prix et carbone par type d'heure",
            "",
            _tableau(
                ["Couleur", "Période", "Heures", "Prix TTC", "CO₂ moyen"],
                [
                    [
                        s.couleur,
                        "creuses" if s.periode == "hc" else "pleines",
                        str(s.nb_heures),
                        prix(s.prix),
                        co2(s.co2_moyen),
                    ]
                    for s in rapport.synthese
                ],
            ),
            "",
        ]
    return blocs


def _creneaux(rapport: RapportMensuel, graphiques: _Graphiques) -> list[str]:
    n = rapport.options.heures_creneau
    critere = (
        "le prix, puis l'intensité carbone"
        if rapport.prix_disponibles
        else "l'intensité carbone (prix indisponibles)"
    )
    blocs = [
        "## 3. Les meilleurs créneaux pour consommer",
        "",
        f"Plages de {n} heures consécutives, classées selon {critere}.",
        "",
        *graphiques.image("profil_horaire", "Profil horaire moyen du mois"),
    ]
    entetes = ["Créneau", "Tarif", "Prix moyen", "CO₂ moyen"]

    def _lignes(creneaux: list[Creneau]) -> list[list[str]]:
        return [
            [plage(c.debut, c.fin), creneau_tarif(c), prix(c.prix_moyen), co2(c.co2_moyen)]
            for c in creneaux
        ]

    if rapport.meilleurs_creneaux:
        blocs += [
            "### À privilégier",
            "",
            _tableau(entetes, _lignes(rapport.meilleurs_creneaux)),
            "",
        ]
    if rapport.pires_creneaux:
        blocs += ["### À éviter", "", _tableau(entetes, _lignes(rapport.pires_creneaux)), ""]
    if rapport.economie is not None:
        e = rapport.economie
        reference = rapport.options.heure_reference
        cout = (
            f"Coût : **{euros(e.cout_optimise)}** au lieu de {euros(e.cout_reference)}, soit "
            f"**{euros(e.gain_euros)} d'économie**. "
            if e.gain_euros is not None
            else ""
        )
        blocs += [
            "### Et si vous décaliez vos usages ?",
            "",
            f"Une consommation flexible de {nombre(e.kwh_par_jour)} kWh par jour (recharge de "
            "voiture électrique, chauffe-eau, électroménager programmable), placée chaque jour "
            f"sur le meilleur créneau de {n} h au lieu de {heure(reference)}–"
            f"{heure(reference + n)}, sur {e.nb_jours} jours : {cout}Émissions : "
            f"**{nombre(e.co2_optimise_kg, 1)} kg de CO₂** au lieu de "
            f"{nombre(e.co2_reference_kg, 1)} kg ({nombre(e.gain_co2_kg, 1)} kg évités).",
            "",
        ]
    return blocs


def _methodologie(rapport: RapportMensuel) -> list[str]:
    o = rapport.options
    return [
        "## Méthodologie et sources",
        "",
        "- **Intensité CO₂** : taux publié par RTE dans éCO2mix (gCO₂/kWh produit en France, "
        "hors imports), données consolidées puis temps réel ; moyenne horaire pondérée par la "
        "production.",
        f"- **Pics** : heures strictement au-dessus du quantile {o.quantile_pic:.0%} de "
        "l'intensité du mois ; des heures de pic consécutives forment un épisode.",
        "- **Tempo** : couleur des jours publiée par l'API RTE « Tempo Like Supply Contract » ; "
        "prix TTC du kWh selon la grille réglementée en vigueur (heures creuses 22 h–6 h), hors "
        "abonnement.",
        "- **Économie estimée** : chaque jour, l'usage flexible est placé sur le créneau le moins "
        f"cher (puis le moins carboné) et comparé au créneau débutant à {heure(o.heure_reference)}."
        " Estimation indicative.",
        "",
        "*Sources : RTE éCO2mix via ODRÉ (Licence Ouverte Etalab), API RTE Tempo. Rapport généré "
        f"le {rapport.genere_le:%d/%m/%Y à %H:%M}.*",
        "",
    ]


def rendre(rapport: RapportMensuel, chemin: Path, *, graphiques: bool = True) -> Path:
    """Écrit le rapport Markdown (et ses graphiques si demandé) ; renvoie son chemin."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    figures = _Graphiques(chemin, rapport, graphiques)
    blocs = [
        f"# Électricité en France — {nom_mois(rapport.mois)}",
        "",
        "*Bilan mensuel : pics de carbone et meilleurs créneaux Tempo*",
        "",
        *_avertissements(rapport),
    ]
    if not rapport.a_des_donnees:
        blocs += [
            "Aucune donnée éCO2mix disponible pour ce mois : le rapport ne peut pas être établi.",
            "",
        ]
    else:
        blocs += [
            *_en_bref(rapport),
            *_carbone(rapport, figures),
            *_tempo(rapport, figures),
            *_creneaux(rapport, figures),
        ]
    blocs += _methodologie(rapport)
    chemin.write_text("\n".join(blocs), encoding="utf-8")
    return chemin
