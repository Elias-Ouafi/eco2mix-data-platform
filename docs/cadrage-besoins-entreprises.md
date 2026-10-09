# Cadrage : de quoi les entreprises ont-elles réellement besoin ?

> Recherche menée le 9 octobre 2026 pour arbitrer le nouvel objectif du projet.
> Hypothèse de départ à tester : *« les entreprises cherchent où l'électricité est
> la moins chère et la plus décarbonée, pour appuyer une stratégie de
> décarbonation à visée marketing. »*
>
> **Verdict : le besoin est réel, mais la question géographique est mal posée.**
> En France, ni le prix ni le contenu carbone ne dépendent significativement de
> l'adresse. Ce qui a de la valeur est la granularité **temporelle** (heure par
> heure), et — pour le seul volet implantation — la **capacité de raccordement**.

---

## 1. Les deux hypothèses, testées

### 1.1 « Le prix de l'électricité varie selon l'endroit » → faux en France

La France applique la **péréquation tarifaire**. Le TURPE (tarif d'acheminement,
20 à 40 % de la facture d'un professionnel) est *identique en tout point du
territoire national*, selon le principe dit du « timbre-poste » : le tarif ne
dépend pas de la distance parcourue par l'électricité ni de la densité de la
zone. Cette péréquation s'étend aux **zones non interconnectées** (Corse, DROM,
îles du Ponant) : deux consommateurs de même profil et de même offre paient le
même tarif, l'État couvrant l'écart de coût de production via les charges de
service public.

Ce qui fait réellement varier le prix d'un site à l'autre :

| Facteur | Varie selon l'adresse ? |
|---|---|
| TURPE (acheminement) | **Non** — péréqué nationalement |
| Domaine de tension de raccordement (BT ≤ 36 kVA, BT > 36 kVA, HTA, HTB) | Non — selon la puissance souscrite |
| Volume et forme de la courbe de charge | Non — selon l'usage |
| Part fourniture (prix du fournisseur, contrat, PPA) | Non — négociée, nationale |
| Taxes et accises | Non — nationales |
| Appartenance à une **ELD** (≈ 130 entreprises locales de distribution, ~5 % du territoire, ~3 à 3,8 M d'habitants) | **Oui, à la marge** — grilles réseau propres |
| **ZNI** (Corse, outre-mer) | Oui, mais en sens inverse : seuls les tarifs réglementés existent, péréqués |

**Conséquence produit :** un service « entrez une adresse → prix moyen de
l'électricité » renverrait, pour ~95 % du territoire français, **la même valeur
partout**. L'information serait vraie mais vide, et laisserait croire à une
variabilité géographique qui n'existe pas.

**Où la question du prix redevient géographique : à l'échelle européenne.** Les
écarts entre zones de marché sont massifs. Pour les consommateurs non
domestiques au 2ᵉ semestre 2025 (Eurostat), le prix moyen UE est de
18,37 €/100 kWh, avec la Finlande à 7,48 et l'Irlande à 25,52 — soit un facteur
3,4. Sur la tranche industrielle 20 000–70 000 MWh : France 7,82 c€/kWh,
Pologne 10,52, Allemagne 13,63. La Pologne cumule par ailleurs la part de taxes
non récupérables la plus élevée de l'UE (36 %).

### 1.2 « Le contenu carbone varie selon l'endroit » → faux aussi, en France

Le réseau français est interconnecté : une région consomme massivement de
l'électricité produite ailleurs, et les flux interrégionaux équilibrent le
système. Le mix *produit* dans une région ne décrit donc pas le mix *consommé*.
Conséquences :

- l'**ADEME (Base Empreinte)** publie **un seul** facteur réglementaire pour le
  « mix électrique France continentale », moyenne annuelle tous usages,
  intégrant pertes réseau et soldes d'échange horaires ;
- utiliser un facteur régional pour un site situé ailleurs oblige à **dégrader
  explicitement la note d'incertitude** du bilan carbone ;
- RTE publie bien un jeu régional (`eco2mix-regional-tr` : consommation,
  production par filière, flux), mais **l'estimation d'émissions `taux_co2` est
  publiée au niveau national**, pas régional. Les intensités régionales
  existantes (ex. le service tiers `carbon-fr`, 12 régions au pas 15 min) sont
  des reconstructions, utiles en analyse, pas opposables en reporting.

**Conséquence produit :** « entrez une adresse → taux de décarbonation moyen
annuel » revient à afficher le facteur national ADEME, déjà public et gratuit,
avec une précision géographique illusoire. C'est le seul livrable que je
recommande de **ne pas** construire.

---

## 2. Ce que les entreprises cherchent vraiment (par priorité)

### Priorité 1 — Le carbone **heure par heure**, pas en moyenne annuelle

C'est là que se déplace toute la réglementation et tous les standards :

- **GHG Protocol, révision du Scope 2** : la proposition centrale introduit une
  exigence de **correspondance horaire** (*hourly matching*) et de
  **deliverability** (la production revendiquée doit se trouver dans la même
  région électrique — typiquement la zone de marché — que la consommation) pour
  la méthode *market-based*. Statut au 9 octobre 2026 : **encore une
  proposition**. La première consultation s'est clôturée le 31 janvier 2026,
  un projet révisé est attendu courant 2026, et la publication finale est
  annoncée pour fin 2027. Le retour de consultation est critique (l'EY plaide
  pour un *hourly matching* optionnel tant que les données ne suivent pas), mais
  la direction est claire.
- **ESRS E1-6 (CSRD)** : le **double reporting** *location-based* **et**
  *market-based* est **maintenu** après l'Omnibus. Publier une seule méthode =
  non-conformité. À noter : l'Omnibus I ((UE) 2026/470, en vigueur le
  18 mars 2026) a réduit le périmètre d'environ 80 % (seuils : > 1 000 salariés
  **et** > 450 M€ de CA, exercices ouverts à partir du 1ᵉʳ janvier 2028) et
  allégé les ESRS de ~61 % des points de données. Moins d'entreprises
  assujetties, mais leurs **donneurs d'ordre leur demanderont les données**.
- **Certificats granulaires / 24/7 CFE** : EnergyTag standardise les certificats
  horaires (*Granular Certificates*), la Climate Group pousse les membres RE100
  vers le *hourly matching*, et un pilote FlexiDAO/Google a converti un
  portefeuille mondial en certificats horaires. Une enquête du Granular
  Certificate Trading Alliance annonçait ~17 TWh de certificats horaires visés
  en Amérique du Nord d'ici 2026 (chiffre prévisionnel, à recouper).

→ **Une moyenne annuelle ne sert plus à rien ; une série au pas 15 minutes est
exactement ce dont ces standards ont besoin.** C'est déjà ce que le projet
ingère.

### Priorité 2 — Quand consommer : décalage de charge et flexibilité

Le seul levier qui produise à la fois une économie immédiate et une baisse
d'émissions mesurable. C'est aussi le point où les deux dimensions se
rejoignent : heures creuses / Tempo pour le prix, heures bas-carbone pour les
émissions — et les deux ne coïncident pas toujours. Le projet dispose déjà des
briques (`fct_creneau_horaire`, calendrier Tempo, grille tarifaire).

### Priorité 3 — La faisabilité d'implantation : le raccordement, pas le prix

Pour un gros consommateur (data center, industrie électro-intensive), c'est
**le** critère, et c'est le seul qui soit franchement géographique :

- « c'est le réseau électrique, plus que le foncier, qui décide où s'installe un
  data center en France » ;
- en mai 2026, **~18 GW de capacité réservés pour ~80 projets** de data centers,
  contre ~5 GW pour une quarantaine de projets fin 2024 ;
- délais de raccordement **2 à 7 ans** selon RTE (le Sénat cite 5 à 7 ans) ;
- seuil structurant : **< 40 MW → Enedis**, au-delà → RTE ; zone grise 20–40 MW
  si le poste source local est saturé ;
- géographie réelle : **Île-de-France saturée** (~7 GW réservés) mais toujours
  première zone de projets, Hauts-de-France deuxième pôle (~6 GW), sites
  préparés au sud de l'IdF (~1 200 MW) et vers Plan-de-Campagne (~500 MW) ;
- le guide ministériel recommande explicitement de **cibler les sites facilement
  raccordables et d'éviter les régions les plus tendues** ;
- cadre qui durcit : loi du 26 mai 2026 (réservation de capacité, déclaration de
  performance environnementale, obligation de valorisation de la chaleur
  fatale), fiche repère DRIEAT applicable au 1ᵉʳ juillet 2026 faisant de la
  capacité de raccordement un critère d'instruction, et RTE qui envisage
  d'abandonner le « premier arrivé, premier servi ».

L'atout français reste le mix : **19,6 gCO₂eq/kWh en 2025** selon EDF, une
électricité « à 95 % décarbonée, fiable et relativement bon marché » (Sénat) —
mais cet atout est **national**, pas local.

### Priorité 4 — Une preuve défendable, parce que l'allégation est un risque

L'angle « marketing » de l'hypothèse initiale est précisément le plus exposé :

- la directive **EmpCo (UE) 2024/825** est applicable dans l'UE depuis le
  **27 septembre 2026** ; la France ne l'a **pas encore transposée** (mise en
  demeure de la Commission le 28 mai 2026, DGCCRF annonçant ne pas contrôler sur
  la base de la directive avant intégration au Code de la consommation) — mais
  le droit existant (pratiques commerciales trompeuses, loi Climat et
  résilience, loi AGEC) sanctionne déjà l'essentiel ;
- une fois transposée : allégations génériques (« vert », « écologique »,
  « bon pour le climat ») interdites sans preuve d'une performance
  environnementale excellente et reconnue, et **la compensation ne pourra plus
  justifier une neutralité carbone** ;
- sur l'électricité précisément, l'ADEME vise le mécanisme des **garanties
  d'origine** : un fournisseur peut en acheter à l'étranger tout en
  s'approvisionnant en fossile en France. D'où le label **VertVolt**.

→ Un outil qui sort un chiffre sans méthode traçable est un **passif** pour son
utilisateur. La valeur se joue autant sur la traçabilité méthodologique que sur
le chiffre.

### Priorité 5 — Le prix, en structure et en comparaison de zones

Utile en comparaison inter-pays (cf. § 1.1) et en décomposition de facture
(fourniture / TURPE / taxes, par domaine de tension et profil). Inutile en
géographie intra-française.

---

## 3. Réponse à la question posée

> *« Faut-il le prix, le niveau de décarbonation, ou les deux ? »*

**Les deux comptent — mais aucune des deux sous la forme « moyenne annuelle à une
adresse ».** Reformulation nécessaire :

| Hypothèse initiale | Ce qu'il faut livrer à la place |
|---|---|
| Prix moyen de l'électricité à cette adresse | Coût **par profil de consommation** (tension, volume, courbe), décomposé fourniture / TURPE / taxes — et comparaison **entre zones de marché européennes** si l'arbitrage est international |
| Taux de décarbonation moyen annuel à cette adresse | **Série horaire / 15 min** d'intensité carbone + **score de couverture 24/7 (CFE)** de la courbe de charge, avec méthode documentée (location-based ADEME/RTE *et* market-based) |
| L'adresse comme déterminant du prix et du carbone | L'adresse comme **clé de résolution de contexte** : commune/INSEE → région → gestionnaire (Enedis / ELD / ZNI) → poste source → **capacité de raccordement disponible et délai** |

L'adresse garde donc tout son sens **comme point d'entrée**, mais ce qu'elle doit
résoudre, c'est le contexte réseau — la seule information locale réellement non
triviale.

---

## 4. Trois objectifs possibles pour le projet

### Option A — Empreinte Scope 2 horaire et score 24/7 *(recommandée)*

L'entreprise fournit sa courbe de charge (ou choisit un profil type) ; le projet
calcule son empreinte heure par heure, son taux de couverture bas-carbone, les
heures les plus émettrices, et le gain d'un décalage de charge.

- **Pour qui :** toute entreprise devant publier un Scope 2 (ESRS E1-6) ou
  préparant le *hourly matching* du GHG Protocol.
- **Pourquoi c'est solide :** adossé à des standards qui se durcissent, et
  réutilise presque tout l'existant (`mix_unifie` au pas 15 min, Tempo,
  `fct_creneau_horaire`, le rapport PDF).
- **Risque :** marché concurrentiel (Electricity Maps, WattTime, FlexiDAO) — mais
  pour un portfolio, se mesurer à ces acteurs est un atout, pas un défaut.

### Option B — Carte d'aide à l'implantation

Adresse → gestionnaire de réseau, poste source, capacité d'accueil restante,
délai indicatif, mix régional, et niveau de tension requis selon la puissance.

- **Pour qui :** data centers, industriels, développeurs de sites.
- **Pourquoi c'est pertinent :** répond au critère n° 1 réel, et c'est la seule
  dimension authentiquement géographique.
- **Risque :** Caparéseau publie à la maille **poste source**, les capacités sont
  explicitement **indicatives et sans engagement**, et la table de
  correspondance commune → poste source reste à construire. Les délais varient
  selon les sources (2–7 ans).

### Option C — Hybride : contexte local + simulateur temporel

Adresse → contexte réseau (option B, en lecture simple) **et** simulateur
carbone/coût horaire (option A) sur un profil de consommation.

- **Pourquoi :** conserve l'intuition initiale (« j'entre une adresse »), tout en
  mettant la valeur là où elle est réellement.
- **Risque :** deux chantiers de données au lieu d'un.

**Recommandation : A comme cœur, B comme module géographique ultérieur** — soit
l'option C livrée en deux temps. Et dans tous les cas, afficher la méthode et
ses limites à côté de chaque chiffre, ce qui est à la fois une exigence EmpCo et
un argument de crédibilité.

---

## 5. Sources de données mobilisables

| Besoin | Source | État |
|---|---|---|
| Intensité carbone nationale 15 min | `eco2mix-national-tr` / `-cons-def` (ODRÉ) | **déjà ingéré** |
| Mix régional | `eco2mix-regional-tr` (ODRÉ) | à ajouter — **pas de `taux_co2` régional officiel** |
| Facteur réglementaire | ADEME Base Empreinte, mix France continentale | à ajouter en seed, versionné |
| Géocodage adresse → code INSEE | API Adresse BAN, **nouveau endpoint `https://data.geopf.fr/geocodage/search`** (l'ancien a été décommissionné fin janvier 2026) | à ajouter — 50 req/s par IP |
| Consommation par commune et secteur NAF | Open Data Enedis, maille commune, 2011–2024 | à ajouter — seuils de confidentialité (≥ 10 sites, > 50 MWh pour les pros) ; **API `data.enedis.fr` non joignable lors du test du 9/10/2026 (HTTP 404 en v1 et v2.1), à reconfirmer** |
| Courbes agrégées > 36 kVA au pas 30 min | Open Data Enedis `conso-sup36` | candidat pour les profils types |
| Capacité d'accueil réseau | Caparéseau (RTE + GRD) | **injection (production) uniquement** — voir § 5 bis |
| Prix non domestiques par pays et tranche | Eurostat `NRG_PC_205` (semestriel) | à ajouter si volet européen |
| Tarifs réseau | Barèmes TURPE 7 (CRE), période 1ᵉʳ août 2025 → 31 juillet 2029 | seed versionné |
| Tarifs Tempo | déjà en seed | **déjà ingéré** |

---

## 6. Faisabilité de l'option B, vérifiée sur les API (9 octobre 2026)

L'option B suppose de répondre, pour une adresse : *« puis-je raccorder ma
consommation ici, à quelle puissance et dans quel délai ? »* La vérification des
catalogues open data impose une correction importante.

### Le constat bloquant

**Il n'existe aucune donnée ouverte de capacité de raccordement disponible pour
la *consommation* (soutirage) en France continentale, à la maille poste.**

| Source | Ce qu'elle couvre réellement |
|---|---|
| **Caparéseau** (RTE + GRD, mensuel) | **Injection** : capacités d'accueil pour raccorder de la *production* EnR en HTB/HTA. Ne répond pas à la question du soutirage. |
| Jeu data.gouv « Capacités d'accueil du réseau » (101 enregistrements) | **ZNI uniquement** : Corse, Martinique, Guadeloupe, Réunion, Guyane. Publié par EDF SEI, pas par Enedis. Hors sujet pour le continent. |
| Carte RTE des capacités pour le **stockage** | La plus proche : capacité en MW par poste, **valable en soutirage comme en injection**. Meilleur approximant quantitatif disponible. |
| Cartes RTE **zones de mutualisation / capacités d'accueil pour la consommation** | Portent bien sur la consommation, mais publiées comme cartes sur le portail services-rte (périmètre de zone, postes concernés, quote-part), pas comme jeu open data exploitable. |
| RTE **SDDR 2025, fiche 5 « Raccordement de l'industrie »** | Zones favorables aux projets de ~250 MW, ~750 MW et ~1 GW (400 kV). Maille grossière, format PDF. |
| Catalogue **ODRÉ** (interrogé par API) | Aucun jeu de capacité de raccordement, ni injection ni soutirage. |

À cela s'ajoutent deux limites structurelles, valables même pour les sources
ci-dessus : les capacités publiées sont **indicatives et non engageantes**, à
confirmer lors de l'instruction de la demande ; et **elles ne s'additionnent
pas** — pour des postes d'une même zone électrique, la capacité prise sur l'un
n'est plus disponible sur les autres.

### Ce qui est réellement disponible, et exploitable

Interrogation directe de l'API ODRÉ, réponses vérifiées :

| Jeu ODRÉ | Volume | Apport pour l'option B |
|---|---|---|
| `consommation-annuelle-par-iris` | 17 891 | **Pièce maîtresse** : consommation annuelle des **sites industriels raccordés au réseau de transport**, avec `code_iris`, `code_insee_commune`, `commune`, `code_insee_departement`, `consommation_electricite_rte`, `pdl_electricite_rte`. Jointure directe avec le géocodage BAN. |
| `energies-et-puissances-regionales-liees-au-contraintes` | 12 | Signal de **contrainte réseau** par région : puissance EnR installée, puissance à compenser, énergie non évacuée par saison. |
| `equilibre-regional-mensuel-prod-conso-brute` | 1 872 | Région **excédentaire ou déficitaire** (production, consommation brute, solde des échanges), avec `geo_shape_region` et `geo_point_region` pour la cartographie. |
| `soutirages-regionaux-quotidiens-consolides-rpt` | 170 726 | Soutirage régional réel sur le réseau de transport, quotidien. |
| `soutirages-quotidiens-consolides-rpt-par-section` | 37 986 | Même chose ventilé par section d'activité (NAF). |
| `eco2mix-regional-cons-def` | 2 859 546 | Équivalent régional du jeu déjà ingéré : mix régional consolidé/définitif. |

### Conséquence sur la définition de l'option B

L'option B **ne peut pas** promettre « la capacité de raccordement disponible à
cette adresse ». Elle peut en revanche livrer, honnêtement et entièrement sur
données ouvertes, un **diagnostic de contexte d'implantation** :

1. adresse → géocodage BAN → `code INSEE` commune + IRIS ;
2. **pression industrielle locale** : consommation des sites industriels
   raccordés au transport dans la commune / le département ;
3. **tension du réseau régional** : excédent ou déficit production/consommation,
   énergie non évacuée, puissance à compenser ;
4. **mix et intensité carbone de la région**, au pas 15 min ;
5. **coût horaire d'un profil de consommation** (Tempo + TURPE) ;
6. **seuil de raccordement applicable** (< 40 MW → Enedis, au-delà → RTE) et
   renvoi explicite vers la demande d'étude de raccordement, seule réponse
   opposable sur la capacité.

Autrement dit : le produit cartographie **le contexte et le risque**, et dit
clairement que **le chiffre de capacité ne s'obtient que par une étude RTE ou
Enedis**. C'est défendable, et c'est exactement le type de limite qu'un outil
exposé aux règles EmpCo doit afficher.

---

## 7. Décision (9 octobre 2026)

| Arbitrage | Choix retenu |
|---|---|
| Objectif du projet | **Option B — aide à l'implantation**, dans la version « diagnostic de contexte » définie au § 5 bis |
| Volet prix | **Coût horaire sur profil de consommation** (Tempo + TURPE), déjà amorcé par les seeds existants |
| Volet prix européen (Eurostat) | écarté pour l'instant |
| Scope 2 horaire / score 24/7 (option A) | écarté comme objectif principal ; les briques 15 min restent en place et resteront réutilisables |
| Interface | **ligne de commande uniquement** — l'adresse est un argument d'entrée (`--adresse`). Aucun site, aucune application : confirmé par l'utilisateur le 9 octobre 2026, dans la continuité du « ne fais pas de site ! » du 4 octobre. |

Ce qui change concrètement pour la plateforme :

- l'ingestion passe de « national » à « national + régional + territorial » ;
- une étape de **géocodage** et une dimension **géographique** (commune, IRIS,
  département, région) entrent dans le modèle ;
- la couche `gold` s'oriente vers des tables de **diagnostic par territoire**
  plutôt que vers le seul rapport mensuel ;
- le rapport PDF existant devient un **rapport d'implantation par adresse**.

Ce qui ne change pas : l'architecture médaillon, DuckDB, dbt, Airflow,
l'idempotence par MERGE, et la trajectoire de migration BigQuery.

---

## 8. Limites et pièges à documenter dans le produit

1. **Pas de facteur d'émission régional opposable** : afficher une intensité
   régionale sans avertissement serait méthodologiquement faux.
2. **La péréquation tarifaire rend le prix non géographique en France** : le dire
   explicitement est un argument de sérieux, pas un aveu de faiblesse.
3. **Capacités Caparéseau indicatives**, à confirmer auprès de RTE/Enedis.
4. **ELD et ZNI** sont des cas particuliers à traiter ou à exclure explicitement.
5. **Marginal ≠ moyen** : les données marginales (WattTime) répondent à « quel
   est l'effet d'un décalage de charge », les moyennes (Electricity Maps, RTE,
   ADEME) à « quelle est mon empreinte ». Mélanger les deux est une erreur
   classique — un acteur du marché affiche lui-même les deux méthodes sur des
   pages différentes.
6. **Le *hourly matching* n'est pas encore la règle** : le présenter comme
   obligatoire serait faux au 9 octobre 2026. Il faut le présenter comme une
   anticipation.

---

## 9. Sources

**Standards et comptabilité carbone**
- [GHG Protocol — consultation Scope 2](https://ghgprotocol.org/blog/release-ghg-protocol-opens-public-consultations-scope-2-and-electricity-sector-consequential) · [document de consultation (PDF)](https://ghgprotocol.org/sites/default/files/2025-10/GHG-Protocol-Scope2-Public-Consultation.pdf) · [aperçu des révisions](https://ghgprotocol.org/blog/upcoming-scope-2-public-consultation-overview-revisions)
- [EnergyTag — 7 messages clés sur la révision Scope 2](https://energytag.org/ghg-protocol-scope-2-update-7-key-messages/) · [FAQ](https://energytag.org/faq/) · [Global Hourly Energy Accounting Initiative](https://energytag.org/projects/global-hourly-energy-accounting-initiative/)
- [EY — ce que signifie la consultation Scope 2](https://www.ey.com/en_gl/insights/ifrs/what-the-ghg-protocol-scope-2-consultation-means) · [KPMG](https://kpmg.com/xx/en/our-insights/ifrg/2025/ghg-protocol-scope2-consultation.html) · [PwC (PDF)](https://viewpoint.pwc.com/dt/us/en/pwc/in_briefs/2025/assets/ibint202521.pdf.coredownload.pdf)
- [Nature Communications — temporal matching comme principe comptable](https://www.nature.com/articles/s41467-025-65125-z)
- [Canary Media — le 24/7 CFE peut-il devenir un standard mondial ?](https://www.canarymedia.com/articles/corporate-procurement/can-24-7-carbon-free-energy-become-a-global-standard) · [Utility Dive — 17 TWh de certificats horaires visés](https://www.utilitydive.com/news/granular-certificate-trading-alliance-survey-levelten/735342/)

**CSRD / ESRS**
- [Dechert — CSRD et CSDDD après l'Omnibus I](https://www.dechert.com/knowledge/onpoint/2026/9/simplification-in-action---eu-csrd-and-csddd-after-the-omnibus-i.html) · [PwC — directive Omnibus finalisée](https://viewpoint.pwc.com/gx/en/pwc/in-briefs/ib_int202527.html) · [EY — simplifications ESRS proposées par l'EFRAG (PDF)](https://www.ey.com/content/dam/ey-unified-site/ey-com/en-gl/technical/csrd-technical-resources/documents/ey-gl-efrag-proposes-major-esrs-simplifications-01-2026.pdf)
- [Scope 2 location-based vs market-based sous ESRS E1-6](https://bindler.co/guides/scope-2-location-based-vs-market-based-esrs-e1-6/) · [ESRS E1 après révision](https://www.coolset.com/academy/esrs-e1-requirements-climate-change) · [PwC — mesure du Scope 2](https://viewpoint.pwc.com/dt/us/en/pwc/sustainability-reporting-guide/sustainability-reporting-guide/srg/srg7/76_scope_2_measurement.html)

**Prix et tarifs**
- [RTE — comprendre le TURPE en 5 infos clés](https://www.rte-france.com/bases-electricite/systeme-electrique/comprendre-turpe-5-infos-cles) · [Observatoire de l'électricité — TURPE](https://observatoire-electricite.fr/systeme-electrique/article/qu-est-ce-que-le-tarif-d-utilisation-des-reseaux-publics-d-electricite-turpe)
- [Eurostat — prix de l'électricité non domestiques, S2 2025](https://ec.europa.eu/eurostat/web/products-eurostat-news/w/ddn-20260508-2) · [Eurostat — statistiques de prix](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Electricity_price_statistics) · [Statista — prix non domestiques par pays (NRG_PC_205)](https://www.statista.com/statistics/1046605/non-household-electricity-prices-european-union-country/)
- [CGE — revue de dépenses sur la péréquation tarifaire des ZNI (PDF)](https://www.economie.gouv.fr/files/files/directions_services/cge/perequation-tarifaire.pdf) · [CRE — tarifs réseaux pour les ELD](https://www.cre.fr/actualites/toute-lactualite/tarifs-reseaux-pour-les-entreprises-locales-de-distribution.html) · [ZNI (Wikipédia)](https://fr.wikipedia.org/wiki/Zones_non_interconnect%C3%A9es_au_r%C3%A9seau_m%C3%A9tropolitain_continental) · [Qu'est-ce qu'une ELD ?](https://www.connaissancedesenergies.org/questions-et-reponses-energies/distribution-et-fourniture-denergie-quest-ce-quune-eld)

**Implantation et raccordement**
- [Guide ministériel d'implantation des centres de données (PDF)](https://www.entreprises.gouv.fr/files/files/Publications/2025/Guide/25112025__Guide%20Datacenters.pdf) · [Sénat — encadrer l'implantation des centres de données (PDF)](https://www.senat.fr/rap/l25-435/l25-435-syn.pdf) · [RTE — les data centers en chiffres clés](https://www.rte-france.com/bases-electricite/consommation-electricite/essor-data-centers-france)
- [RTE — raccordement et transport des data centers (PDF)](https://www.institutparisregion.fr/fileadmin/DataStorage/SavoirFaire/NosTravaux/Amenagement/webinairesDataCenters/2/5_RTE_raccordement_transport_DC.pdf) · [EDF — stratégie data centers (PDF)](https://www.institutparisregion.fr/fileadmin/DataStorage/SavoirFaire/NosTravaux/Amenagement/webinairesDataCenters/2/7_EDF_strategie_DC.pdf) · [Usine Nouvelle — RTE envisage d'abandonner le « premier arrivé, premier servi »](https://www.usinenouvelle.com/electronique-informatique/cloud-computing/datacenters/sortir-de-la-regle-du-premier-demandeur-premier-servi-rte-envisage-de-changer-de-politique-pour-reduire-les-delais-de-raccordement-electrique-des-datacenters.RDEXNYUWVNAXTHKGFVVU5FYL24.html) · [DCmag — fiche repère DRIEAT](https://dcmag.fr/fiche-repere-drieat-data-centers-en-ile-de-france-un-changement-de-posture-logique-du-cote-de-letat/) · [Gossement Avocats — loi du 26 mai 2026](https://www.gossement-avocats.com/blog/centres-de-donnees-data-centers-ce-que-change-la-loi-de-simplification-de-la-vie-economique/)

**Allégations environnementales**
- [ADEME — guide anti-greenwashing, édition 2025 (PDF)](https://communication-responsable.ademe.fr/sites/default/files/2025-12/Guide%20anti-greenwashing%20de%20l'ADEME_%C3%A9dition%202025_0.pdf) · [AFNOR — label VertVolt](https://www.afnor.org/en/news/label-electricite-vertevolt-revele-the-color-of-your-electricity/)
- [CMS — suivi de transposition d'EmpCo en France](https://cms.law/en/int/expert-guides/cms-implementation-tracker-for-the-empco-directive/france) · [Victoris Avocat — ce qui change au 27 septembre 2026](https://www.victorisavocat.com/blog/allegations-environnementales-27-septembre-2026-directive-empco)

**Données carbone et outils existants**
- [ODRÉ — éCO2mix national temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/?flg=en-us) · [RTE — données régionales éCO2mix](https://www.rte-france.com/en/data-publications/eco2mix/regional-data) · [ADEME — mix électrique France continentale (Base Empreinte)](https://prod-basecarbonesolo.ademe-dri.fr/documentation/UPLOAD_DOC_FR/electricite_reglementaire.htm) · [carbon-fr — intensité carbone par région](https://carbon-fr.kovelt.fr/)
- [Electricity Maps — API](https://www.electricitymaps.com/platform/api) · [documentation](https://app.electricitymaps.com/api/docs/reference) · [WattTime — moyen vs marginal](https://watttime.org/data-science/data-signals/average-vs-marginal/) · [FlexiDAO — reporting 24/7 granulaire](https://www.electricitymaps.com/resources/case-studies/flexidao-enables-customers-to-report-their-granular-24-7-emissions)

**Données mobilisables**
- [API Adresse (BAN) — transfert à l'IGN](https://adresse.data.gouv.fr/blog/lapi-adresse-de-la-base-adresse-nationale-est-transferee-a-lign) · [fiche data.gouv](https://www.data.gouv.fr/dataservices/api-adresse-base-adresse-nationale-ban)
- [Enedis — consommation annuelle par secteur et commune](https://data.enedis.fr/explore/dataset/consommation-electrique-par-secteur-dactivite-commune/api/) · [agrégats > 36 kVA au pas 30 min](https://data.enedis.fr/explore/dataset/conso-sup36/api/) · [cartographie des capacités](https://opendata.enedis.fr/pages/cartographie)
- [data.gouv — capacités d'accueil du réseau (Caparéseau)](https://www.data.gouv.fr/datasets/capacites-daccueil-du-reseau)
