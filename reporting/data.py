"""Chiffres du rapport mensuel, calculés sur `gold.fct_creneau_horaire`.

Toutes les requêtes lisent la couche gold en lecture seule : le rapport ne
recalcule aucune règle métier (couleur Tempo, période tarifaire, prix), il ne
fait que sélectionner, classer et résumer.

Définitions retenues, reprises dans la section méthodologie du PDF :

* **Pic de carbone** : heure dont l'intensité dépasse strictement le 9e décile du
  mois (une égalité au seuil, fréquente sur un plateau, n'est pas un pic). Des
  heures de pic consécutives forment un *épisode*.
* **Créneau** : fenêtre de `heures_creneau` heures consécutives, toutes mesurées.
  Les meilleurs créneaux sont classés par prix moyen puis par intensité carbone ;
  sans calendrier Tempo, par intensité carbone seule.
* **Usage flexible** : une consommation quotidienne déplaçable (recharge de
  véhicule, électroménager programmable). Chaque jour, elle est placée sur le
  meilleur créneau de la journée et comparée à un créneau de référence en soirée.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import duckdb

from ingestion.transform import PARIS_TZ

COULEURS = ("bleu", "blanc", "rouge")


@dataclass(frozen=True, slots=True)
class Options:
    """Paramètres du rapport, exposés dans le DAG et la CLI."""

    #: Durée d'un créneau, en heures.
    heures_creneau: int = 3
    #: Consommation flexible déplacée chaque jour, en kWh.
    usage_flexible_kwh: float = 7.0
    #: Heure locale de début du créneau de référence (soirée, retour à domicile).
    heure_reference: int = 18
    #: Quantile définissant un pic de carbone.
    quantile_pic: float = 0.9


@dataclass(frozen=True, slots=True)
class Heure:
    heure_paris: datetime
    taux_co2: float | None
    couleur: str | None
    periode: str
    prix: float | None


@dataclass(frozen=True, slots=True)
class Episode:
    """Suite d'heures consécutives au-dessus du seuil de pic."""

    debut: datetime
    fin: datetime
    co2_max: float
    co2_moyen: float

    @property
    def duree_heures(self) -> int:
        # En UTC : entre deux heures de Paris de même tzinfo, Python soustrait
        # l'heure murale et se tromperait d'une heure au changement d'heure.
        return round((self.fin.astimezone(UTC) - self.debut.astimezone(UTC)) / timedelta(hours=1))


@dataclass(frozen=True, slots=True)
class Creneau:
    debut: datetime
    fin: datetime
    prix_moyen: float | None
    co2_moyen: float
    couleur: str | None
    periode: str


@dataclass(frozen=True, slots=True)
class SyntheseTarif:
    couleur: str
    periode: str
    nb_heures: int
    prix: float | None
    co2_moyen: float | None


@dataclass(frozen=True, slots=True)
class Economie:
    """Usage flexible placé au meilleur créneau du jour, comparé à la soirée."""

    nb_jours: int
    kwh_par_jour: float
    cout_optimise: float | None
    cout_reference: float | None
    co2_optimise_kg: float
    co2_reference_kg: float

    @property
    def gain_euros(self) -> float | None:
        if self.cout_optimise is None or self.cout_reference is None:
            return None
        return self.cout_reference - self.cout_optimise

    @property
    def gain_co2_kg(self) -> float:
        return self.co2_reference_kg - self.co2_optimise_kg


@dataclass(slots=True)
class RapportMensuel:
    mois: date
    options: Options
    genere_le: datetime
    heures: list[Heure]
    heures_attendues: int
    heures_completes: int
    jours_du_mois: int
    jours_tempo_connus: int
    co2_moyen: float | None = None
    heure_plus_propre: Heure | None = None
    heure_plus_carbonee: Heure | None = None
    seuil_pic: float | None = None
    episodes: list[Episode] = field(default_factory=list)
    jours_plus_carbones: list[tuple[date, float]] = field(default_factory=list)
    calendrier: dict[date, str] = field(default_factory=dict)
    repartition_tempo: dict[str, int] = field(default_factory=dict)
    jours_rouges: list[date] = field(default_factory=list)
    synthese: list[SyntheseTarif] = field(default_factory=list)
    profil_horaire: list[tuple[int, float | None, float | None]] = field(default_factory=list)
    meilleurs_creneaux: list[Creneau] = field(default_factory=list)
    pires_creneaux: list[Creneau] = field(default_factory=list)
    economie: Economie | None = None

    @property
    def a_des_donnees(self) -> bool:
        return any(h.taux_co2 is not None for h in self.heures)

    @property
    def prix_disponibles(self) -> bool:
        return any(h.prix is not None for h in self.heures)

    @property
    def couverture(self) -> float:
        return self.heures_completes / self.heures_attendues if self.heures_attendues else 0.0


def _bornes_mois(mois: date) -> tuple[date, date]:
    suivant = date(mois.year + 1, 1, 1) if mois.month == 12 else date(mois.year, mois.month + 1, 1)
    return mois, suivant


def _heures_attendues(mois: date) -> int:
    """Heures du mois civil en heure de Paris (743 ou 745 aux changements d'heure)."""
    debut, fin = _bornes_mois(mois)
    start = datetime(debut.year, debut.month, debut.day, tzinfo=PARIS_TZ).astimezone(UTC)
    end = datetime(fin.year, fin.month, fin.day, tzinfo=PARIS_TZ).astimezone(UTC)
    return round((end - start) / timedelta(hours=1))


def _paris(value: datetime) -> datetime:
    """DuckDB renvoie l'heure de Paris en TIMESTAMP naïf : on la rend explicite."""
    return value.replace(tzinfo=PARIS_TZ) if value.tzinfo is None else value.astimezone(PARIS_TZ)


def collect(
    connection: duckdb.DuckDBPyConnection,
    mois: date,
    options: Options | None = None,
    *,
    now: datetime | None = None,
) -> RapportMensuel:
    """Rassemble tous les chiffres du rapport d'un mois civil."""
    options = options or Options()
    mois = mois.replace(day=1)
    debut, fin = _bornes_mois(mois)
    params = {"mois": mois}

    rows = connection.execute(
        """
        SELECT heure_paris, taux_co2_g_kwh, couleur_tempo, periode_tarifaire,
               prix_ttc_eur_kwh, est_complete
        FROM gold.fct_creneau_horaire
        WHERE mois = $mois
        ORDER BY heure_utc
        """,
        params,
    ).fetchall()
    heures = [Heure(_paris(r[0]), r[1], r[2], r[3], r[4]) for r in rows]

    jours_tempo = connection.execute(
        """
        SELECT jour_tempo, couleur FROM silver.tempo_jours
        WHERE jour_tempo >= $debut AND jour_tempo < $fin
        ORDER BY jour_tempo
        """,
        {"debut": debut, "fin": fin},
    ).fetchall()

    rapport = RapportMensuel(
        mois=mois,
        options=options,
        genere_le=(now or datetime.now(PARIS_TZ)).astimezone(PARIS_TZ),
        heures=heures,
        heures_attendues=_heures_attendues(mois),
        heures_completes=sum(1 for r in rows if r[5]),
        jours_du_mois=(fin - debut).days,
        jours_tempo_connus=len(jours_tempo),
        calendrier=dict(jours_tempo),
        repartition_tempo={c: sum(1 for _, col in jours_tempo if col == c) for c in COULEURS},
        jours_rouges=[jour for jour, couleur in jours_tempo if couleur == "rouge"],
    )
    if not rapport.a_des_donnees:
        return rapport

    _carbone(connection, rapport, params)
    _tarifs(connection, rapport, params)
    _creneaux(connection, rapport, params)
    _economie(connection, rapport, params)
    return rapport


def _carbone(connection: duckdb.DuckDBPyConnection, rapport: RapportMensuel, params) -> None:
    mesurees = [h for h in rapport.heures if h.taux_co2 is not None]
    rapport.co2_moyen = sum(h.taux_co2 for h in mesurees) / len(mesurees)
    rapport.heure_plus_propre = min(mesurees, key=lambda h: h.taux_co2)
    rapport.heure_plus_carbonee = max(mesurees, key=lambda h: h.taux_co2)

    rapport.seuil_pic = connection.execute(
        "SELECT quantile_cont(taux_co2_g_kwh, $q) FROM gold.fct_creneau_horaire WHERE mois = $mois",
        {**params, "q": rapport.options.quantile_pic},
    ).fetchone()[0]

    # Îlots d'heures consécutives au-dessus du seuil (technique gaps and islands).
    episodes = connection.execute(
        """
        WITH pics AS (
            SELECT heure_utc, heure_paris, taux_co2_g_kwh,
                   heure_utc - to_hours(row_number() OVER (ORDER BY heure_utc)) AS ilot
            FROM gold.fct_creneau_horaire
            WHERE mois = $mois AND taux_co2_g_kwh > $seuil
        )
        SELECT min(heure_paris), max(heure_paris) + INTERVAL 1 HOUR,
               max(taux_co2_g_kwh), avg(taux_co2_g_kwh)
        FROM pics
        GROUP BY ilot
        ORDER BY max(taux_co2_g_kwh) DESC, min(heure_utc)
        LIMIT 8
        """,
        {**params, "seuil": rapport.seuil_pic},
    ).fetchall()
    rapport.episodes = [Episode(_paris(a), _paris(b), c, d) for a, b, c, d in episodes]

    rapport.jours_plus_carbones = connection.execute(
        """
        SELECT jour, avg(taux_co2_g_kwh) AS co2
        FROM gold.fct_creneau_horaire
        WHERE mois = $mois AND taux_co2_g_kwh IS NOT NULL
        GROUP BY jour
        ORDER BY co2 DESC
        LIMIT 5
        """,
        params,
    ).fetchall()


def _tarifs(connection: duckdb.DuckDBPyConnection, rapport: RapportMensuel, params) -> None:
    rows = connection.execute(
        """
        SELECT couleur_tempo, periode_tarifaire, count(*), avg(prix_ttc_eur_kwh),
               avg(taux_co2_g_kwh)
        FROM gold.fct_creneau_horaire
        WHERE mois = $mois AND couleur_tempo IS NOT NULL
        GROUP BY ALL
        """,
        params,
    ).fetchall()
    ordre = {(c, p): i for i, (c, p) in enumerate((c, p) for c in COULEURS for p in ("hc", "hp"))}
    rapport.synthese = sorted(
        (SyntheseTarif(*row) for row in rows), key=lambda s: ordre[(s.couleur, s.periode)]
    )
    rapport.profil_horaire = connection.execute(
        """
        SELECT heure_locale, avg(taux_co2_g_kwh), avg(prix_ttc_eur_kwh)
        FROM gold.fct_creneau_horaire
        WHERE mois = $mois
        GROUP BY heure_locale
        ORDER BY heure_locale
        """,
        params,
    ).fetchall()


def _fenetres_sql(n: int) -> str:
    """Fenêtres glissantes de `n` heures. `n` est un entier : interpolation sûre."""
    if not isinstance(n, int) or n < 1:
        raise ValueError(f"durée de créneau invalide : {n!r}")
    return f"""
    WITH fenetres AS (
        SELECT
            heure_utc,
            heure_paris,
            jour,
            heure_locale,
            couleur_tempo,
            periode_tarifaire,
            avg(prix_ttc_eur_kwh) OVER w AS prix_moyen,
            avg(taux_co2_g_kwh) OVER w AS co2_moyen,
            count(taux_co2_g_kwh) OVER w AS nb_mesurees,
            count(prix_ttc_eur_kwh) OVER w AS nb_prix,
            last_value(heure_utc) OVER w AS derniere
        FROM gold.fct_creneau_horaire
        WHERE mois = $mois
        WINDOW w AS (ORDER BY heure_utc ROWS BETWEEN CURRENT ROW AND {n - 1} FOLLOWING)
    )
    SELECT * FROM fenetres
    -- Fenêtre complète : n heures mesurées et contiguës (aucun trou de données).
    WHERE nb_mesurees = {n} AND derniere = heure_utc + to_hours({n - 1})
"""


def _creneaux(connection: duckdb.DuckDBPyConnection, rapport: RapportMensuel, params) -> None:
    n = rapport.options.heures_creneau
    avec_prix = rapport.prix_disponibles
    filtre = f"nb_prix = {n}" if avec_prix else "true"
    cle = "prix_moyen, co2_moyen" if avec_prix else "co2_moyen"
    cle_desc = "prix_moyen DESC, co2_moyen DESC" if avec_prix else "co2_moyen DESC"

    def _top(ordre: str, k: int) -> list[Creneau]:
        rows = connection.execute(
            f"SELECT heure_paris, prix_moyen, co2_moyen, couleur_tempo, periode_tarifaire "
            f"FROM ({_fenetres_sql(n)}) WHERE {filtre} ORDER BY {ordre}, heure_utc",
            params,
        ).fetchall()
        # Sélection gloutonne de fenêtres disjointes : la meilleure, puis la
        # meilleure qui ne la chevauche pas, etc.
        retenus: list[Creneau] = []
        for debut, prix, co2, couleur, periode in rows:
            debut = _paris(debut)
            fin = debut + timedelta(hours=n)
            if all(fin <= c.debut or debut >= c.fin for c in retenus):
                retenus.append(Creneau(debut, fin, prix, co2, couleur, periode))
            if len(retenus) == k:
                break
        return retenus

    rapport.meilleurs_creneaux = _top(cle, 5)
    rapport.pires_creneaux = _top(cle_desc, 3)


def _economie(connection: duckdb.DuckDBPyConnection, rapport: RapportMensuel, params) -> None:
    options = rapport.options
    n = options.heures_creneau
    avec_prix = rapport.prix_disponibles
    filtre = f"nb_prix = {n}" if avec_prix else "true"
    ordre = "prix_moyen, co2_moyen" if avec_prix else "co2_moyen"
    rows = connection.execute(
        f"""
        WITH fenetres AS ({_fenetres_sql(n)}),
        candidates AS (SELECT * FROM fenetres WHERE {filtre}),
        meilleure AS (
            SELECT jour, prix_moyen, co2_moyen FROM candidates
            QUALIFY row_number() OVER (PARTITION BY jour ORDER BY {ordre}, heure_utc) = 1
        ),
        reference AS (
            SELECT jour, prix_moyen, co2_moyen FROM candidates
            WHERE heure_locale = $heure_reference
            QUALIFY row_number() OVER (PARTITION BY jour ORDER BY heure_utc) = 1
        )
        SELECT count(*),
               sum(meilleure.prix_moyen), sum(reference.prix_moyen),
               sum(meilleure.co2_moyen), sum(reference.co2_moyen)
        FROM meilleure
        INNER JOIN reference USING (jour)
        """,
        {**params, "heure_reference": options.heure_reference},
    ).fetchone()
    nb_jours, prix_opt, prix_ref, co2_opt, co2_ref = rows
    if not nb_jours:
        return
    kwh = options.usage_flexible_kwh
    rapport.economie = Economie(
        nb_jours=nb_jours,
        kwh_par_jour=kwh,
        cout_optimise=None if prix_opt is None else prix_opt * kwh,
        cout_reference=None if prix_ref is None else prix_ref * kwh,
        # g/kWh x kWh = g ; / 1000 pour des kilogrammes.
        co2_optimise_kg=co2_opt * kwh / 1000,
        co2_reference_kg=co2_ref * kwh / 1000,
    )
