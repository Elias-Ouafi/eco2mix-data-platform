# Feuille de route

> Établie le 9 octobre 2026, après la réorientation du projet vers le diagnostic
> d'implantation électrique par adresse (voir
> [cadrage-besoins-entreprises.md](cadrage-besoins-entreprises.md)).
>
> Deux parties : **A.** le projet complet, lot par lot. **B.** le MVP, dans
> l'ordre, en coupant tout ce qui n'est pas indispensable à la démonstration.

**Rappel du périmètre** : aucun site, aucune application. L'utilisateur clone le
dépôt, lance une commande et passe l'adresse en argument.

---

## Partie A — Le projet complet

### Lot 0 — Socle technique · **fait**

| # | Étape | État |
|---|---|---|
| 0.1 | Client API ODRÉ : retry, timeout, quota compté et quota serveur relevé | fait |
| 0.2 | Normalisation UTC avec gestion des deux changements d'heure | fait |
| 0.3 | Architecture médaillon : bronze Python, silver et gold dbt-duckdb | fait |
| 0.4 | Interface `WarehouseLoader` + `DuckDBLoader`, MERGE idempotent | fait |
| 0.5 | Clé de MERGE **composite** et colonnes typées par nature | fait (9 oct.) |
| 0.6 | Extraction intégrale pour les jeux sans dimension temporelle | fait (9 oct.) |
| 0.7 | Correction du BOM dans les exports CSV + backfill de réparation | fait (9 oct.) |
| 0.8 | Géocodage BAN : `geocode --adresse` | fait (9 oct.) |
| 0.9 | Trois DAGs Airflow, CI (ruff, pytest, import DagBag) | fait, Airflow jamais exécuté |
| 0.10 | Rapport mensuel PDF et Markdown | fait, sans prix réels |

### Lot 1 — Données territoriales

| # | Étape | Fini quand | Dépend de |
|---|---|---|---|
| 1.1 | ~~Déclarer les sources dbt des jeux territoriaux~~ — **fait** (9 oct.), 8 tests de source ajoutés | ~~`dbt build` passe~~ 74/74 verts | — |
| 1.2 | ~~Charger `consommation-annuelle-par-iris`~~ — **fait** (9 oct.) : 17 656 lignes après dédoublonnage, 2012→2023, 1 523 communes | ~~La table bronze est peuplée~~ | 1.1 |
| 1.3 | ~~Charger `equilibre-regional-mensuel` et `contraintes-regionales`~~ — **fait** (9 oct.) : 1 872 et 12 lignes | ~~Tables bronze peuplées~~ | 1.1 |
| 1.4 | ~~Ajouter les trois specs à `BRONZE_SPECS`~~ — **fait** (9 oct.) | ~~Les tables existent dès le premier run~~ | 1.1 |
| 1.5 | Charger `eco2mix-regional-cons-def` (2,86 M lignes, 1 appel export) | Série régionale au pas 15 min disponible | 1.1 |
| 1.6 | ~~Référentiel territoires : commune → département → région~~ — **fait** (10 oct.) : seeds `dim_region`, `dim_departement`, macro `code_departement` | ~~Un `code_insee_commune` résout sa hiérarchie sans appel réseau~~ | — |
| 1.7 | ~~Silver : normaliser les noms de région et joindre **par code INSEE**~~ — **fait** (10 oct.), plus trois réparations découvertes sur les données réelles : codes IRIS tronqués à 8 caractères, doublons vides, communes non publiées | ~~Les trois jeux se joignent sans perte~~ | 1.2, 1.3, 1.6 |
| 1.8 | ~~Tests dbt sur les nouvelles tables silver~~ — **fait** (10 oct.) : 134 tests verts sur données réelles, dont la conformité de la hiérarchie aux codes publiés | ~~Tests verts sur données réelles~~ | 1.7 |

### Lot 2 — Couche de diagnostic (gold)

| # | Étape | Fini quand |
|---|---|---|
| 2.1 | ~~`fct_pression_industrielle_commune` et `_departement`~~ — **fait** (10 oct.), millésime = dernière année sans secret statistique (2021) | ~~Une requête par code INSEE renvoie la pression locale~~ |
| 2.2 | ~~`fct_tension_reseau_region`~~ — **fait** (10 oct.) : couverture sur 12 mois glissants, rang de dépendance, contraintes d'évacuation | ~~Les régions se classent par tension~~ |
| 2.3 | ~~Seuils de raccordement~~ — **fait** (10 oct.) : seed `seuils_raccordement` (BT, HTA, zone grise 20–40 MW, HTB) | ~~Une puissance de projet renvoie le gestionnaire et le niveau de tension~~ |
| 2.4 | `fct_mix_regional` : mix et parts bas-carbone par région, au pas 15 min | Série régionale exploitable |
| 2.5 | ~~`fct_diagnostic_territoire`~~ — **fait** (10 oct.), **une ligne par département** et non par commune : aucun jeu ouvert ne liste les 35 000 communes, et seules ~1 300 ont un site. La commune s'ajoute par jointure à la lecture | ~~Un seul `SELECT` produit tout le diagnostic~~ |
| 2.6 | ~~Limites méthodologiques~~ — **fait** (10 oct.) : seed `limites_methodologiques` | ~~Chaque chiffre du rapport peut citer sa limite~~ |

### Lot 3 — Coût horaire d'un profil

| # | Étape | Fini quand | Dépend de |
|---|---|---|---|
| 3.1 | Seed `profils_consommation` : courbes types (tertiaire, industriel 3×8, data center, froid alimentaire) — **amorcé** (10 oct.) : un profil illustratif, bureau 36 kVA | Quatre profils sélectionnables | — |
| 3.2 | ~~Vérifier la grille Tempo sur source officielle~~ — **fait** (10 oct.) : les 12 prix recalculés depuis les barèmes HT de la CRE (TTC = (HT + accise) × 1,20) | ~~`tarifs_tempo.csv` sourcé et daté~~ | — |
| 3.3 | Seed `turpe` : barèmes TURPE 7 par domaine de tension (CRE, période août 2025 → juillet 2029) | Composantes fixes et variables par tension | — |
| 3.4 | Identifiants API RTE Tempo (compte `data.rte-france.com`) | Le calendrier Tempo se charge sur données réelles | **action utilisateur** |
| 3.5 | ~~`fct_cout_horaire_profil`~~ — **fait** (10 oct.) sur l'option HP/HC du tarif bleu non résidentiel (fourniture et TURPE inclus dans le TRV, plus l'accise, en € HTVA) | ~~Un profil est chiffré sur un mois~~ |
| 3.6 | ~~Gain de décalage~~ — **fait** (10 oct.) dans `fct_cout_mensuel_profil` : report de la part flexible vers les heures creuses du jour. Constat : en été, ce report **augmente** les émissions | Le meilleur créneau de décalage est identifié | 3.5 |

### Lot 4 — Restitution en ligne de commande

| # | Étape | Fini quand |
|---|---|---|
| 4.1 | ~~`diagnose --adresse`~~ — **fait** (10 oct.) | ~~Une adresse produit un diagnostic lisible~~ |
| 4.2 | Options ~~`--puissance`~~ (**fait**, 10 oct.) et `--profil` | Le diagnostic s'adapte au projet décrit |
| 4.3 | `--format pdf` / `--format md` : rapport d'implantation par adresse | Un document est produit, graphiques compris |
| 4.4 | `--format json` / `--format csv` : sortie machine | Le diagnostic s'intègre à un autre outil |
| 4.5 | ~~Affichage systématique des limites méthodologiques~~ — **fait** en sortie terminal (10 oct.), testé | ~~Aucun indicateur n'apparaît sans sa limite~~ |

### Lot 5 — Exploitation et orchestration

| # | Étape | Fini quand | Dépend de |
|---|---|---|---|
| 5.1 | Lancer la stack Astro (`astro dev start`) et valider les DAGs | Le DAG horaire est vert plusieurs heures de suite | **Docker Desktop à installer** |
| 5.2 | DAG `territorial_refresh` (annuel/mensuel selon les jeux) | Les jeux territoriaux se rafraîchissent seuls | 1.x |
| 5.3 | Alerte en cas d'échec de tâche | Un échec simulé déclenche l'alerte | 5.1 |
| 5.4 | Protéger `main` (CI obligatoire) | Aucun push direct possible | — |
| 5.5 | Exploitation 7 jours, puis compléter les chiffres clés du README | Le tableau est complet | 5.1 |

### Lot 6 — Vitrine

| # | Étape | Fini quand |
|---|---|---|
| 6.1 | Exemple de rapport d'implantation commité (PDF + Markdown) | Le dépôt se comprend sans rien installer |
| 6.2 | Captures des graphiques dans le README | — |
| 6.3 | ~~Renommer le dépôt et aligner le nom du projet partout~~ — **fait** (9 oct. 2026) : « Où brancher mon entreprise », slug `ou-brancher-mon-entreprise` | ~~Plus aucune occurrence de l'ancien nom~~ |
| 6.4 | Démonstration de 2 minutes (asciinema ou vidéo courte) | Un prospect voit tourner l'outil |

### Lot 7 — Reporté, assumé

| Sujet | Pourquoi plus tard |
|---|---|
| Migration GCS + BigQuery | `BigQueryLoader` à écrire derrière l'interface existante ; aucun intérêt fonctionnel tant que le local suffit |
| Comparaison européenne (Eurostat `NRG_PC_205`) | Donne du sens au « où est-ce moins cher » à l'échelle où la question a une réponse, mais élargit le périmètre |
| Scope 2 horaire et score 24/7 CFE | Écarté comme objectif ; les briques 15 min restent en place |
| IRIS par jointure spatiale | Inutile : `code_insee_commune` suffit aux jointures |
| Intensité carbone régionale | **Impossible proprement** : pas de `taux_co2` régional chez RTE, pas de facteur ADEME régional opposable |
| Capacité de raccordement chiffrée | **Absente de l'open data** pour la consommation ; seule une étude RTE/Enedis fait foi |

---

## Partie B — Le MVP, dans l'ordre

**Ce que le MVP doit prouver** : une adresse française en entrée, un diagnostic
d'implantation étayé par des données réelles en sortie, reproductible par
quiconque clone le dépôt.

**Deux coupes qui font gagner le plus de temps :**

1. **Pas d'Airflow dans le MVP.** La ligne de commande fait tout, et Docker est
   justement le blocage actuel. Les DAGs existent déjà et sont vérifiés en CI :
   les lancer est une étape d'exploitation, pas de démonstration.
2. **Pas de mix régional au pas 15 min** (2,86 M lignes). Comme il n'existe de
   toute façon pas d'intensité carbone régionale opposable, cette série
   n'apporte presque rien au diagnostic, pour le plus gros coût d'ingestion.

### Les sept étapes

| # | Étape | Pourquoi elle est dans le MVP | Effort |
|---|---|---|---|
| **1** | ~~Sources dbt + chargement des trois jeux territoriaux~~ — **FAIT le 9 octobre 2026** (4 appels API) | Sans elles, `--adresse` ne renvoyait que des codes administratifs. | ~~½ journée~~ |
| **2** | ~~Référentiel commune → département → région, et silver normalisée jointe **par code INSEE**~~ — **FAIT le 10 octobre 2026**. Repli au département quand la commune n'a pas de site, signalé dans la sortie. | Les sources écrivent `GRAND EST` et `Grand Est` : sans normalisation, la jointure perd des lignes en silence. | ~~½ journée~~ |
| **3** | ~~`fct_diagnostic_territoire`~~ — **FAIT le 10 octobre 2026**, une ligne par **département** (voir 2.5), la commune jointe à la lecture | Le cœur du produit. Un `SELECT` par code INSEE doit suffire. | ~~1 journée~~ |
| **4** | ~~`diagnose --adresse`~~ — **FAIT le 10 octobre 2026**, avec `--puissance` et les limites affichées | C'est le livrable visible, et la forme exacte que tu as fixée. | ~~½ journée~~ |
| **5** | ~~Coût horaire sur **un seul** profil type~~ — **FAIT le 10 octobre 2026**, sur l'option HP/HC (voie sans compte RTE) | Prouve le volet prix sans attendre les quatre profils ni le TURPE détaillé. | 1 journée |
| **6** | ~~Tests : dbt sur les nouvelles tables, pytest sur `diagnose`~~ — **FAIT** (154 tests dbt, 41 tests pytest pour les étapes 2 à 5) | La rigueur est l'argument de vente du dépôt ; une table gold sans test l'annule. | ½ journée |
| **7** | README : un exemple de diagnostic réel, de bout en bout, et le nouveau nom | Le dépôt doit se comprendre sans rien installer. | ½ journée |

**Total : environ 4 à 5 jours à temps partiel.**

### Le point de décision de l'étape 5

Le coût horaire a besoin des couleurs Tempo, donc d'un compte
`data.rte-france.com` (identifiants dans `airflow/.env`). Deux voies :

- **tu crées le compte** (une quinzaine de minutes) → coût Tempo réel, bien plus
  démonstratif ;
- **tu ne le crées pas** → repli sur une tarification Base ou HP/HC, chiffrée
  depuis le seed TURPE, sans dépendance externe. Moins parlant, mais le MVP
  tient quand même.

**Décision du 10 octobre 2026 : HP/HC, sans compte RTE.** La grille vient
directement des barèmes de la CRE (tarif bleu non résidentiel, 36 kVA), qui
incluent déjà l'acheminement : le seed TURPE n'est pas nécessaire pour ce
périmètre. Il le redeviendra pour les sites au-delà de 36 kVA (3.3).

### Ce qui vient juste après le MVP

Dans cet ordre : lancer Airflow (5.1), brancher les quatre profils (3.1), le
TURPE détaillé (3.3), le rapport PDF par adresse (4.3), puis la vitrine (lot 6).
