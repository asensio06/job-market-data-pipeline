# 🎯 Préparation Entretien : Difficultés Techniques & Solutions (STAR Method)

Ce document récapitule les principaux défis techniques rencontrés lors de la réalisation du projet **Job Market Data Pipeline**, ainsi que les solutions apportées. Il est structuré selon la méthode **STAR** (Situation, Tâche, Action, Résultat) idéale pour les entretiens techniques en Data Engineering.

---

## 💡 Question d'entretien classique :
> *"Quelles ont été les principales difficultés techniques rencontrées lors de la réalisation de ce projet de Pipeline de Données, et comment y avez-vous pallié ?"*

---

### 📌 Difficulté 1 : Incompatibilité de versions & Dépendances PySpark / Pandas dans Docker Airflow

- **Situation** : Lors de l'intégration de PySpark au sein du conteneur Airflow (Docker), l'exécution de la tâche `transform_silver` échouait brutalement avec l'erreur : `PySparkImportError: [UNSUPPORTED_PACKAGE_VERSION] Pandas >= 2.2.0 must be installed; however, your version is 2.1.4.`
- **Tâche** : Faire fonctionner PySpark 4.2.0 en harmonie avec Apache Airflow 2.10.4 et Java JRE 17 au sein de conteneurs Docker sans rompre la stabilité d'Airflow.
- **Action** :
  1. J'ai créé un `Dockerfile.airflow` sur-mesure étendant l'image officielle `apache/airflow:2.10.4-python3.12`.
  2. J'y ai installé le paquet système `default-jre-headless` (nécessaire pour la JVM PySpark) et forcé la mise à jour de `pandas>=2.2.0` et `pyarrow>=18.0.0` dans le `requirements.txt`.
  3. J'ai configuré les permissions utilisateur dans `docker-compose.yml` avec `user: "1000:0"` pour éviter tout conflit de droits de fichiers sur le volume monté localement.
- **Résultat** : PySpark s'exécute de manière fluide et rapide à l'intérieur des worker nodes Airflow conteneurisés avec 100% de succès.

---

### 📌 Difficulté 2 : Parsing & Échappement des colonnes JSON imbriquées avec PySpark SQL

- **Situation** : L'API France Travail renvoie des données brutes JSON fortement imbriquées (`entreprise.nom`, `lieuTravail.libelle`, `salaire.libelle`). Lors de la conversion du DataFrame Pandas en DataFrame PySpark, l'analyseur SQL de PySpark levait une `AnalysisException: UNRESOLVED_COLUMN: Could not resolve column entreprise.nom`.
- **Tâche** : Permettre à PySpark d'accéder directement aux champs contenant un point dans leur nom sans les confondre avec des attributs de structures imbriquées (StructType).
- **Action** :
  - J'ai utilisé l'échappement par des backticks PySpark : `F.col('`entreprise.nom`')`.
  - J'ai également réécrit les fonctions de recherche NLP regex (`F.rlike`) pour appliquer l'extraction des compétences tech directement au niveau des colonnes PySpark nativement distribuées.
- **Résultat** : Un code PySpark extrêmement robuste capable de traiter des schémas JSON complexes sans erreurs de résolutions de colonnes.

---

### 📌 Difficulté 3 : Déduplication Multi-Régions & Filtrage Regex Stricte des Écoles (Couche Silver)

- **Situation** : Le scraping d'offres sur deux zones géographiques distinctes (Île-de-France et Lille) générait des offres en doublon. De plus, une grande partie des offres taguées "Alternance" étaient publiées par des organismes de formation / écoles (ex: OpenClassrooms, ISCOD, Studi) et non par les entreprises recruteuses finales.
- **Tâche** : Garantir la haute qualité des données dans la couche Silver en éliminant 100% des doublons inter-régions, en écartant les intermédiaires de formation et en isolant **strictement les contrats CDD d'alternance de 24 mois**.
- **Action** :
  1. Mis en place d'une déduplication sur l'identifiant unique `id_offre` dès la phase d'extraction Bronze.
  2. Création d'un filtre regex d'exclusion d'entreprises dans PySpark : `~F.col('entreprise').rlike('(?i)(ISCOD|KAISCHOOL|OpenClassrooms|Studi|Pigier|Epitech|CFA|École|Campus|Euridis|MBWay|Formation)')`.
  3. Filtrage strict sur la durée du contrat : `dureeContrat == 'CDD - 24 Mois'`.
- **Résultat** : Le dataset Silver ne contient que les vraies opportunités de contrat d'apprentissage de 24 mois auprès d'entreprises directes.

---

### 📌 Difficulté 4 : Agrégation Dynamique de Listes de Compétences NLP dans DuckDB & dbt

- **Situation** : La couche PySpark extrait une liste de compétences tech sous forme de chaîne de caractères séparée par des virgules (ex: `"python,sql,docker"`). Comment modéliser efficacement ces listes dans un Data Warehouse analytique sans créer de schémas relationnels inutilement lourds ?
- **Tâche** : Modéliser un Schéma en Étoile (Star Schema) performant avec dbt pour isoler les compétences tech et permettre des agrégations analytiques rapides.
- **Action** :
  1. J'ai utilisé la fonction vectorisée DuckDB `unnest(string_split(competences_tech, ','))` pour exploser les compétences dynamiquement sous forme de lignes.
  2. J'ai créé un modèle dbt dimensionnel `dim_competences.sql` qui génère des identifiants uniques MD5 pour chaque skill tech.
  3. J'ai créé `fact_offres.sql` reliant les faits aux dimensions `dim_entreprises`, `dim_localisations` et `dim_competences`.
- **Résultat** : Une modélisation dbt propre et modulable avec 6 Data Quality Tests s'exécutant avec succès (`dbt test` PASSED).

---

### 📌 Difficulté 5 : Exécution Agnostique de dbt en Sous-Processus Python (Local vs Docker Airflow)

- **Situation** : Lorsque l'étape Gold (`load_gold.py`) déclenchait `dbt run` via Python (`subprocess.run(['dbt', ...])`), l'exécution échouait dans Docker avec l'erreur `No such file or directory: 'dbt'` ou `Catalog Error: schema duckdb_source does not exist`.
- **Tâche** : Rendre le déclenchement de dbt 100% agnostique de l'environnement (exécutable aussi bien dans un venv Python local que dans les conteneurs Docker Airflow).
- **Action** :
  1. Spécification explicite du schéma `schema: main` dans le fichier de définition de source `src_duckdb.yml` pour DuckDB.
  2. Remplacement de l'appel direct `dbt` par le sous-module Python universel `sys.executable` : `[sys.executable, '-m', 'dbt.cli.main', 'run', ...]`.
- **Résultat** : Le pipeline Medallion orchestre dbt de manière 100% transparente en local comme dans Docker Airflow.

---

## 🛠️ Résumé Synthétique à Répondre à l'Oral

> *"Pour résumer, les principaux défis ont été d'ordre devops et d'ingénierie de données : d'une part, harmoniser l'environnement conteneurisé Docker Airflow avec PySpark et dbt (gestion de l'image custom, du JRE et des dépendances Python), et d'autre part, concevoir un filtrage de nettoyage robuste pour filtrer les écoles et isoler uniquement les alternances de 24 mois. J'ai pallié à cela en mettant en place une architecture dbt en étoile sur DuckDB et en automatisant des tests de qualité de données (dbt test) intégrés directement dans le DAG Airflow."*
