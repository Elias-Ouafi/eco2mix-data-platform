"""Graphiques du rapport mensuel, rendus en PNG (matplotlib, sans affichage)."""

from __future__ import annotations

import calendar
import io
from datetime import timedelta

import matplotlib

matplotlib.use("Agg")  # aucun serveur graphique dans les conteneurs Airflow

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch, Rectangle

from reporting.data import RapportMensuel

#: Couleurs Tempo, adoucies pour rester lisibles en fond de graphique.
TEMPO_HEX = {"bleu": "#2F6DB5", "blanc": "#B8BEC6", "rouge": "#D0312D", None: "#EEEEEE"}
ENCRE = "#1F2933"
GRILLE = "#D9DEE3"
PIC = "#D0312D"
CO2 = "#4A5A6A"
PRIX = "#E08A00"

plt.rcParams.update(
    {
        "font.size": 9,
        "axes.edgecolor": GRILLE,
        "axes.labelcolor": ENCRE,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "xtick.color": ENCRE,
        "ytick.color": ENCRE,
        "axes.grid": True,
        "grid.color": GRILLE,
        "grid.linewidth": 0.6,
    }
)

JOURS = ["lun.", "mar.", "mer.", "jeu.", "ven.", "sam.", "dim."]


def _png(fig: plt.Figure) -> bytes:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return buffer.getvalue()


def courbe_carbone(rapport: RapportMensuel) -> bytes:
    """Intensité carbone heure par heure, pics surlignés, couleur Tempo en fond."""
    heures = [h for h in rapport.heures if h.taux_co2 is not None]
    x = [h.heure_paris.replace(tzinfo=None) for h in heures]
    y = [h.taux_co2 for h in heures]

    fig, ax = plt.subplots(figsize=(7.2, 2.6))
    # Fond : couleur Tempo de chaque heure (jour Tempo de 6 h à 6 h).
    for h in heures:
        if h.couleur is not None:
            debut = h.heure_paris.replace(tzinfo=None)
            ax.axvspan(debut, debut + timedelta(hours=1), color=TEMPO_HEX[h.couleur],
                       alpha=0.12, linewidth=0)  # fmt: skip
    ax.plot(x, y, color=CO2, linewidth=0.9)
    if rapport.seuil_pic is not None:
        ax.axhline(rapport.seuil_pic, color=PIC, linestyle="--", linewidth=0.8)
        pics = [(xi, yi) for xi, yi in zip(x, y, strict=True) if yi > rapport.seuil_pic]
        if pics:
            ax.scatter(*zip(*pics, strict=True), color=PIC, s=6, zorder=3)
        ax.annotate(
            f"seuil de pic : {rapport.seuil_pic:.0f} g/kWh",
            xy=(0.01, 0.97),
            xycoords="axes fraction",
            ha="left",
            va="top",
            color=PIC,
            fontsize=8,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.5},
        )
    ax.set_ylabel("gCO₂/kWh")
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.set_ylim(bottom=0)
    return _png(fig)


def carte_chaleur(rapport: RapportMensuel) -> bytes:
    """Intensité carbone par jour (lignes) et heure locale (colonnes)."""
    nb_jours = rapport.jours_du_mois
    grille = np.full((nb_jours, 24), np.nan)
    for h in rapport.heures:
        if h.taux_co2 is not None:
            # Le jour du retour à l'heure d'hiver, l'heure 2 h apparaît deux fois :
            # on garde la plus carbonée, c'est ce qu'un pic doit montrer.
            jour, heure = h.heure_paris.day - 1, h.heure_paris.hour
            grille[jour, heure] = np.fmax(grille[jour, heure], h.taux_co2)

    fig, ax = plt.subplots(figsize=(7.2, 0.1 * nb_jours + 0.8))
    image = ax.imshow(grille, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax.set_xticks(range(0, 24, 2), [f"{h} h" for h in range(0, 24, 2)])
    ax.set_yticks(range(0, nb_jours, 2), [str(d + 1) for d in range(0, nb_jours, 2)])
    ax.set_xlabel("heure (Paris)")
    ax.set_ylabel("jour du mois")
    ax.grid(False)
    fig.colorbar(image, ax=ax, label="gCO₂/kWh", fraction=0.04, pad=0.02)
    return _png(fig)


def profil_horaire(rapport: RapportMensuel) -> bytes:
    """Intensité carbone moyenne par heure, prix moyen du kWh en surimpression."""
    heures = [p[0] for p in rapport.profil_horaire]
    co2 = [p[1] if p[1] is not None else np.nan for p in rapport.profil_horaire]
    couleurs = ["#8FB3D9" if h >= 22 or h < 6 else "#C9D3DD" for h in heures]

    fig, ax = plt.subplots(figsize=(7.2, 2.6))
    ax.bar(heures, co2, color=couleurs, width=0.8)
    ax.set_xlabel("heure (Paris)")
    ax.set_ylabel("gCO₂/kWh moyen")
    ax.set_xticks(range(0, 24, 2))
    handles = [
        Patch(color="#8FB3D9", label="heures creuses (22 h–6 h)"),
        Patch(color="#C9D3DD", label="heures pleines"),
    ]
    prix = [p[2] for p in rapport.profil_horaire]
    if any(v is not None for v in prix):
        ax2 = ax.twinx()
        ax2.plot(heures, [v if v is not None else np.nan for v in prix], color=PRIX,
                 marker="o", markersize=3, linewidth=1.4)  # fmt: skip
        ax2.set_ylabel("prix moyen (€/kWh)", color=PRIX)
        ax2.tick_params(axis="y", colors=PRIX)
        ax2.grid(False)
        ax2.spines["right"].set_visible(True)
        ax2.set_ylim(bottom=0)
        handles.append(plt.Line2D([], [], color=PRIX, marker="o", label="prix moyen Tempo"))
    ax.legend(handles=handles, loc="upper left", fontsize=7, frameon=False)
    return _png(fig)


def calendrier_tempo(rapport: RapportMensuel) -> bytes:
    """Calendrier du mois, une case par jour colorée selon Tempo."""
    semaines = calendar.Calendar().monthdatescalendar(rapport.mois.year, rapport.mois.month)
    fig, ax = plt.subplots(figsize=(4.2, 0.45 * len(semaines) + 0.5))
    for ligne, semaine in enumerate(semaines):
        for colonne, jour in enumerate(semaine):
            if jour.month != rapport.mois.month:
                continue
            couleur = rapport.calendrier.get(jour)
            y = len(semaines) - 1 - ligne
            ax.add_patch(Rectangle((colonne, y), 0.92, 0.88, color=TEMPO_HEX[couleur]))
            texte = "white" if couleur in ("bleu", "rouge") else ENCRE
            ax.text(colonne + 0.46, y + 0.44, str(jour.day), ha="center", va="center",
                    color=texte, fontsize=8)  # fmt: skip
    ax.set_xlim(0, 7)
    ax.set_ylim(0, len(semaines))
    ax.set_xticks([i + 0.46 for i in range(7)], JOURS)
    ax.xaxis.tick_top()
    ax.set_yticks([])
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(length=0)
    legende = [Patch(color=TEMPO_HEX[c], label=c) for c in ("bleu", "blanc", "rouge")]
    legende.append(Patch(color=TEMPO_HEX[None], label="inconnu"))
    ax.legend(handles=legende, loc="upper center", bbox_to_anchor=(0.5, -0.02), ncol=4,
              fontsize=7, frameon=False)  # fmt: skip
    return _png(fig)
