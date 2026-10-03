"""Mise en page PDF du rapport mensuel (reportlab).

Les polices standard du PDF (Helvetica) ne couvrent que le jeu Latin-1 étendu :
le « 2 » de CO2 passe donc par la balise `<sub>` dans les paragraphes, et les
tableaux écrivent « CO2 » en clair.
"""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from reporting import charts
from reporting.data import Creneau, RapportMensuel

MOIS = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]  # fmt: skip
JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

ENCRE = colors.HexColor("#1F2933")
DISCRET = colors.HexColor("#5F6B76")
FILET = colors.HexColor("#D9DEE3")
FOND = colors.HexColor("#F3F5F7")
ACCENT = colors.HexColor("#2F6DB5")
ALERTE = colors.HexColor("#D0312D")
TEMPO = {
    "bleu": colors.HexColor("#2F6DB5"),
    "blanc": colors.HexColor("#B8BEC6"),
    "rouge": colors.HexColor("#D0312D"),
}

LARGEUR = A4[0] - 36 * mm

_base = getSampleStyleSheet()
STYLES = {
    "titre": ParagraphStyle("titre", parent=_base["Title"], fontSize=20, leading=24,
                            textColor=ENCRE, alignment=TA_LEFT, spaceAfter=2),
    "sous_titre": ParagraphStyle("sous_titre", parent=_base["Normal"], fontSize=11,
                                 textColor=DISCRET, spaceAfter=10),
    "h1": ParagraphStyle("h1", parent=_base["Heading2"], fontSize=14, textColor=ENCRE,
                         spaceBefore=10, spaceAfter=6),
    "h2": ParagraphStyle("h2", parent=_base["Heading3"], fontSize=11, textColor=ENCRE,
                         spaceBefore=8, spaceAfter=4),
    "texte": ParagraphStyle("texte", parent=_base["Normal"], fontSize=9.5, leading=13,
                            textColor=ENCRE, spaceAfter=6),
    "note": ParagraphStyle("note", parent=_base["Normal"], fontSize=8, leading=11,
                           textColor=DISCRET, spaceAfter=4),
    "kpi_valeur": ParagraphStyle("kpi_valeur", parent=_base["Normal"], fontSize=14,
                                 leading=17, textColor=ENCRE, fontName="Helvetica-Bold"),
    "kpi_libelle": ParagraphStyle("kpi_libelle", parent=_base["Normal"], fontSize=8,
                                  leading=10, textColor=DISCRET),
    "cellule": ParagraphStyle("cellule", parent=_base["Normal"], fontSize=8.5, leading=11,
                              textColor=ENCRE),
}  # fmt: skip

CO2 = "CO<sub>2</sub>"


# --- Formats ----------------------------------------------------------------


def nom_mois(mois: date) -> str:
    return f"{MOIS[mois.month - 1]} {mois.year}"


def _nombre(valeur: float, decimales: int = 0) -> str:
    texte = f"{valeur:,.{decimales}f}".replace(",", " ").replace(".", ",")
    return texte


def _euros(valeur: float | None, decimales: int = 2) -> str:
    return "n.d." if valeur is None else f"{_nombre(valeur, decimales)} €"


def _prix(valeur: float | None) -> str:
    return "n.d." if valeur is None else f"{_nombre(valeur, 4)} €/kWh"


def _co2(valeur: float | None) -> str:
    return "n.d." if valeur is None else f"{_nombre(valeur)} g/kWh"


def _jour(valeur: date) -> str:
    return f"{JOURS[valeur.weekday()]} {valeur.day} {MOIS[valeur.month - 1]}"


#: Espace insécable : « 13 h » ne doit jamais être coupé en fin de ligne.
NBSP = chr(0xA0)


def _heure(valeur: datetime | int) -> str:
    heure = valeur if isinstance(valeur, int) else valeur.hour
    return f"{heure}{NBSP}h"


def _plage(debut: datetime, fin: datetime) -> str:
    if debut.date() == fin.date():
        return f"{_jour(debut.date())}, {_heure(debut)}–{_heure(fin)}"
    if fin.hour == 0 and fin.date() - debut.date() == timedelta(days=1):
        # Se termine à minuit : on reste sur le jour de début.
        return f"{_jour(debut.date())}, {_heure(debut)}–{_heure(24)}"
    return f"{_jour(debut.date())} {_heure(debut)} → {_jour(fin.date())} {_heure(fin)}"


def _creneau_tarif(creneau: Creneau) -> str:
    periode = "heures creuses" if creneau.periode == "hc" else "heures pleines"
    return periode if creneau.couleur is None else f"jour {creneau.couleur}, {periode}"


# --- Blocs ------------------------------------------------------------------


def _image(png: bytes, largeur: float = LARGEUR) -> Image:
    image = Image(io.BytesIO(png))
    ratio = image.imageHeight / image.imageWidth
    image.drawWidth, image.drawHeight = largeur, largeur * ratio
    return image


def _tableau(entetes: list[str], lignes: list[list], largeurs: list[float]) -> Table:
    data = [entetes] + [
        [Paragraph(str(c), STYLES["cellule"]) if isinstance(c, str) else c for c in ligne]
        for ligne in lignes
    ]
    table = Table(data, colWidths=largeurs, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8.5),
                ("TEXTCOLOR", (0, 0), (-1, 0), DISCRET),
                ("LINEBELOW", (0, 0), (-1, 0), 0.8, ENCRE),
                ("LINEBELOW", (0, 1), (-1, -1), 0.4, FILET),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _kpis(cartes: list[tuple[str, str]]) -> Table:
    """Bandeau d'indicateurs : valeur en gros, libellé dessous."""
    cellules = [
        [Paragraph(valeur, STYLES["kpi_valeur"]), Paragraph(libelle, STYLES["kpi_libelle"])]
        for valeur, libelle in cartes
    ]
    par_ligne = 3
    lignes = [cellules[i : i + par_ligne] for i in range(0, len(cellules), par_ligne)]
    for ligne in lignes:
        ligne.extend([[""]] * (par_ligne - len(ligne)))
    table = Table(lignes, colWidths=[LARGEUR / par_ligne] * par_ligne, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), FOND),
                ("BOX", (0, 0), (-1, -1), 0.4, FILET),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return table


def _encadre(texte: str, couleur: colors.Color = ACCENT) -> Table:
    table = Table([[Paragraph(texte, STYLES["texte"])]], colWidths=[LARGEUR], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), FOND),
                ("LINEBEFORE", (0, 0), (0, -1), 3, couleur),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return table


# --- Sections ---------------------------------------------------------------


def _avertissements(rapport: RapportMensuel) -> list:
    messages = []
    if rapport.couverture < 0.98:
        messages.append(
            f"Données éCO2mix incomplètes : {rapport.heures_completes} heures mesurées sur "
            f"{rapport.heures_attendues} ({rapport.couverture:.0%}). Les totaux portent sur "
            "les heures disponibles."
        )
    if rapport.jours_tempo_connus < rapport.jours_du_mois:
        manquants = rapport.jours_du_mois - rapport.jours_tempo_connus
        messages.append(
            f"Calendrier Tempo incomplet : {manquants} jour(s) sans couleur. "
            + (
                "Les prix ne sont pas disponibles : les créneaux sont classés sur le seul "
                "critère carbone."
                if not rapport.prix_disponibles
                else "Les prix de ces jours ne sont pas pris en compte."
            )
        )
    elif rapport.a_des_donnees and not rapport.prix_disponibles:
        messages.append(
            "Aucun tarif Tempo connu pour cette période (grille `tarifs_tempo`) : "
            "les créneaux sont classés sur le seul critère carbone."
        )
    return [_encadre("<b>À noter.</b> " + m, ALERTE) for m in messages]


def _synthese(rapport: RapportMensuel) -> list:
    cartes = [
        (_co2(rapport.co2_moyen), f"intensité {CO2} moyenne du mois"),
        (
            _co2(rapport.heure_plus_propre.taux_co2),
            f"heure la plus propre : {_jour(rapport.heure_plus_propre.heure_paris.date())}, "
            f"{_heure(rapport.heure_plus_propre.heure_paris)}",
        ),
        (
            _co2(rapport.heure_plus_carbonee.taux_co2),
            f"pic maximal : {_jour(rapport.heure_plus_carbonee.heure_paris.date())}, "
            f"{_heure(rapport.heure_plus_carbonee.heure_paris)}",
        ),
        (
            f"{rapport.repartition_tempo.get('rouge', 0)} / "
            f"{rapport.repartition_tempo.get('blanc', 0)} / "
            f"{rapport.repartition_tempo.get('bleu', 0)}"
            if rapport.jours_tempo_connus
            else "n.d.",
            "jours rouges / blancs / bleus",
        ),
    ]
    if rapport.meilleurs_creneaux:
        meilleur = rapport.meilleurs_creneaux[0]
        cartes.append(
            (
                _prix(meilleur.prix_moyen)
                if meilleur.prix_moyen is not None
                else _co2(meilleur.co2_moyen),
                f"meilleur créneau : {_plage(meilleur.debut, meilleur.fin)}",
            )
        )
    if rapport.economie is not None:
        economie = rapport.economie
        valeur = (
            _euros(economie.gain_euros)
            if economie.gain_euros is not None
            else f"{_nombre(economie.gain_co2_kg, 1)} kg"
        )
        cartes.append(
            (valeur, f"économisés en déplaçant {_nombre(economie.kwh_par_jour)} kWh par jour")
        )
    return [_kpis(cartes)]


def _section_carbone(rapport: RapportMensuel) -> list:
    blocs: list = [
        Paragraph("1. Pics de carbone", STYLES["h1"]),
        Paragraph(
            f"L'intensité {CO2} mesure les émissions de la production électrique française, "
            f"heure par heure. Une heure est un <b>pic</b> lorsqu'elle fait partie des 10 % les "
            f"plus carbonées du mois — au-delà de <b>{_co2(rapport.seuil_pic)}</b> ce mois-ci. "
            "Ils surviennent lorsque les centrales thermiques (gaz, charbon, fioul) sont le plus "
            "sollicitées, souvent en soirée, quand la production solaire s'efface.",
            STYLES["texte"],
        ),
        _image(charts.courbe_carbone(rapport)),
        Paragraph(
            ("Fond coloré : couleur Tempo de l'heure. " if rapport.jours_tempo_connus else "")
            + "Points rouges : heures de pic.",
            STYLES["note"],
        ),
    ]
    if rapport.episodes:
        blocs += [
            Paragraph("Épisodes de pic les plus intenses", STYLES["h2"]),
            _tableau(
                ["Période", "Durée", "CO2 max", "CO2 moyen"],
                [
                    [
                        _plage(e.debut, e.fin),
                        f"{e.duree_heures} h",
                        _co2(e.co2_max),
                        _co2(e.co2_moyen),
                    ]
                    for e in rapport.episodes
                ],
                [LARGEUR * 0.52, LARGEUR * 0.12, LARGEUR * 0.18, LARGEUR * 0.18],
            ),
        ]
    if rapport.jours_plus_carbones:
        blocs += [
            Paragraph("Jours les plus carbonés", STYLES["h2"]),
            _tableau(
                ["Jour", "CO2 moyen", "Couleur Tempo"],
                [
                    [_jour(j), _co2(v), rapport.calendrier.get(j, "inconnue")]
                    for j, v in rapport.jours_plus_carbones
                ],
                [LARGEUR * 0.45, LARGEUR * 0.25, LARGEUR * 0.30],
            ),
        ]
    blocs += [
        KeepTogether(
            [
                Paragraph("Carte horaire du mois", STYLES["h2"]),
                _image(charts.carte_chaleur(rapport), LARGEUR * 0.92),
            ]
        )
    ]
    return blocs


def _section_tempo(rapport: RapportMensuel) -> list:
    blocs: list = [
        Paragraph("2. Calendrier Tempo et prix", STYLES["h1"]),
        Paragraph(
            "Avec l'option Tempo, le prix du kWh dépend de la couleur du jour, fixée la veille "
            "par RTE, et de l'heure : heures creuses de 22 h à 6 h, heures pleines de 6 h à "
            "22 h. Un jour Tempo court de 6 h à 6 h le lendemain. Les heures pleines des jours "
            "rouges sont de loin les plus chères.",
            STYLES["texte"],
        ),
    ]
    if rapport.jours_tempo_connus:
        blocs.append(_image(charts.calendrier_tempo(rapport), LARGEUR * 0.6))
    else:
        blocs.append(
            Paragraph(
                "Le calendrier Tempo de ce mois n'est pas chargé : relancez le DAG avec le "
                "paramètre <i>tempo</i> activé et des identifiants RTE configurés.",
                STYLES["texte"],
            )
        )
    if rapport.jours_rouges:
        blocs.append(
            Paragraph(
                "<b>Jours rouges :</b> " + ", ".join(_jour(j) for j in rapport.jours_rouges) + ".",
                STYLES["texte"],
            )
        )
    if rapport.synthese:
        blocs += [
            Paragraph("Prix et carbone par type d'heure", STYLES["h2"]),
            _tableau(
                ["Couleur", "Période", "Heures", "Prix TTC", "CO2 moyen"],
                [
                    [
                        s.couleur,
                        "creuses" if s.periode == "hc" else "pleines",
                        str(s.nb_heures),
                        _prix(s.prix),
                        _co2(s.co2_moyen),
                    ]
                    for s in rapport.synthese
                ],
                [LARGEUR * 0.16, LARGEUR * 0.16, LARGEUR * 0.14, LARGEUR * 0.28, LARGEUR * 0.26],
            ),
        ]
    return blocs


def _section_creneaux(rapport: RapportMensuel) -> list:
    n = rapport.options.heures_creneau
    critere = (
        "le prix, puis l'intensité carbone"
        if rapport.prix_disponibles
        else ("l'intensité carbone (prix indisponibles)")
    )
    blocs: list = [
        Paragraph("3. Les meilleurs créneaux pour consommer", STYLES["h1"]),
        Paragraph(
            f"Profil moyen du mois : quelles heures sont, en moyenne, les plus propres et les "
            f"moins chères. Les créneaux ci-dessous sont des plages de {n} heures consécutives, "
            f"classées selon {critere}.",
            STYLES["texte"],
        ),
        _image(charts.profil_horaire(rapport)),
    ]
    entetes = ["Créneau", "Tarif", "Prix moyen", "CO2 moyen"]
    largeurs = [LARGEUR * 0.40, LARGEUR * 0.24, LARGEUR * 0.18, LARGEUR * 0.18]

    def _lignes(creneaux: list[Creneau]) -> list[list[str]]:
        return [
            [_plage(c.debut, c.fin), _creneau_tarif(c), _prix(c.prix_moyen), _co2(c.co2_moyen)]
            for c in creneaux
        ]

    if rapport.meilleurs_creneaux:
        blocs += [
            Paragraph("À privilégier", STYLES["h2"]),
            _tableau(entetes, _lignes(rapport.meilleurs_creneaux), largeurs),
        ]
    if rapport.pires_creneaux:
        blocs += [
            Paragraph("À éviter", STYLES["h2"]),
            _tableau(entetes, _lignes(rapport.pires_creneaux), largeurs),
        ]
    if rapport.economie is not None:
        e = rapport.economie
        reference = rapport.options.heure_reference
        cout = (
            f"Coût : <b>{_euros(e.cout_optimise)}</b> au lieu de {_euros(e.cout_reference)}, "
            f"soit <b>{_euros(e.gain_euros)} d'économie</b>. "
            if e.gain_euros is not None
            else ""
        )
        blocs += [
            Spacer(1, 6),
            _encadre(
                f"<b>Et si vous décaliez vos usages ?</b> Une consommation flexible de "
                f"{_nombre(e.kwh_par_jour)} kWh par jour (recharge de voiture électrique, "
                f"chauffe-eau, électroménager programmable), placée chaque jour sur le meilleur "
                f"créneau de {n}{NBSP}h au lieu de "
                f"{_heure(reference)}–{_heure(reference + n)}, sur "
                f"{e.nb_jours} jours : {cout}Émissions : <b>{_nombre(e.co2_optimise_kg, 1)} kg "
                f"de {CO2}</b> au lieu de {_nombre(e.co2_reference_kg, 1)} kg "
                f"({_nombre(e.gain_co2_kg, 1)} kg évités)."
            ),
        ]
    return blocs


def _methodologie(rapport: RapportMensuel) -> list:
    return [
        Paragraph("Méthodologie et sources", STYLES["h1"]),
        Paragraph(
            f"<b>Intensité {CO2}</b> : taux publié par RTE dans éCO2mix (gCO<sub>2</sub>/kWh "
            "produit en France, hors imports), données consolidées puis temps réel. "
            "L'intensité d'une heure est la moyenne de ses mesures pondérée par la production.",
            STYLES["note"],
        ),
        Paragraph(
            f"<b>Pics</b> : heures strictement au-dessus du quantile "
            f"{rapport.options.quantile_pic:.0%} "
            "de l'intensité du mois ; des heures de pic consécutives forment un épisode.",
            STYLES["note"],
        ),
        Paragraph(
            "<b>Tempo</b> : couleur des jours publiée par l'API RTE « Tempo Like Supply "
            "Contract » ; prix TTC du kWh selon la grille réglementée en vigueur à la date "
            "(heures creuses 22 h–6 h), hors abonnement. Le prix réellement payé dépend du "
            "contrat et de la puissance souscrite.",
            STYLES["note"],
        ),
        Paragraph(
            "<b>Économie estimée</b> : chaque jour, l'usage flexible est placé sur le créneau "
            "le moins cher (puis le moins carboné) de la journée et comparé au créneau de "
            f"référence débutant à {_heure(rapport.options.heure_reference)}. "
            "Estimation indicative.",
            STYLES["note"],
        ),
        Paragraph(
            "Sources : RTE éCO2mix via ODRÉ (Licence Ouverte Etalab), API RTE Tempo. "
            f"Rapport généré le {rapport.genere_le:%d/%m/%Y à %H:%M}.",
            STYLES["note"],
        ),
    ]


def _pied_de_page(titre: str):
    def dessiner(canvas, document) -> None:
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(DISCRET)
        canvas.drawString(18 * mm, 10 * mm, titre)
        canvas.drawRightString(A4[0] - 18 * mm, 10 * mm, f"page {document.page}")
        canvas.restoreState()

    return dessiner


def rendre(rapport: RapportMensuel, chemin: Path) -> Path:
    """Écrit le rapport PDF et renvoie son chemin."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    titre = f"Électricité en France — {nom_mois(rapport.mois)}"
    document = SimpleDocTemplate(
        str(chemin),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=18 * mm,
        title=titre,
        subject="Pics de carbone et meilleurs créneaux Tempo",
        author="eco2mix-data-platform",
    )
    blocs: list = [
        Paragraph(titre, STYLES["titre"]),
        Paragraph(
            "Bilan mensuel : pics de carbone et meilleurs créneaux Tempo", STYLES["sous_titre"]
        ),
        *_avertissements(rapport),
    ]
    if not rapport.a_des_donnees:
        blocs.append(
            Paragraph(
                "Aucune donnée éCO2mix disponible pour ce mois : le rapport ne peut pas être "
                "établi. Vérifiez que les DAGs d'ingestion ont tourné sur la période.",
                STYLES["texte"],
            )
        )
    else:
        blocs += [
            Spacer(1, 4),
            *_synthese(rapport),
            *_section_carbone(rapport),
            PageBreak(),
            *_section_tempo(rapport),
            *_section_creneaux(rapport),
        ]
    blocs += _methodologie(rapport)
    pied = _pied_de_page(titre)
    document.build(blocs, onFirstPage=pied, onLaterPages=pied)
    return chemin
