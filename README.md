# ⚡ Job Market Data Engineering Pipeline (Île-de-France & Lille)

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.0-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.10-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker_Compose-24.0+-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30+-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)

> **Plateforme d'Ingénierie de Données End-to-End** basée sur l'architecture **Medallion (Bronze ➔ Silver ➔ Gold)** pour collecter, nettoyer avec **PySpark**, modéliser en Étoile avec **dbt**, stocker dans **DuckDB**, orchestrer via **Apache Airflow dans Docker**, et restituer sur un **Dashboard Streamlit** avec alertes instantanées **Telegram**.

---

## 📌 Sommaire
1. [Présentation & Valeur Métier](#-présentation--valeur-métier)
2. [Architecture du Pipeline](#-architecture-du-pipeline)
3. [Stack Technique & Les 5 Piliers](#-stack-technique--les-5-piliers)
4. [Modélisation en Étoile (dbt Star Schema)](#-modélisation-en-étoile-dbt-star-schema)
5. [Structure du Dépôt](#-structure-du-dépôt)
6. [Guide d'Installation & d'Exécution](#-guide-dinstallation--dexécution)
7. [Visualisation & Alertes](#-visualisation--alertes)
8. [Qualité des Données & Tests](#-qualité-des-données--tests)

---

## 🎯 Présentation & Valeur Métier

Le marché de l'emploi dans la Data et l'IA est extrêmement dynamique, mais les offres d'alternance sont souvent parasitées par des annonces d'écoles/organismes de formation ou des durées de contrat non souhaitées.

Ce projet résout ce problème en construisant un pipeline automatique qui :
- **Extrait quotidiennement** les opportunités Data/IA sur deux zones stratégiques : **Île-de-France** et **Lille / Nord**.
- **Filtre avec une rigueur absolue** : Conserve **uniquement les contrats d'alternance de 24 mois** auprès d'entreprises directes (élimination à 100% des écoles et centres de formation).
- **Extrait les compétences Tech via NLP (PySpark)** : Analyse les textes pour détecter la demande réelle en technos (*Python, SQL, PySpark, Airflow, Docker, Snowflake, BigQuery, dbt, PowerBI, AWS, etc.*).
- **Alerte en temps réel** sur Telegram avec les liens de postulation directe.

---

## 🏛️ Architecture du Pipeline

```text
 +---------------------------------------------------------+
 |           API France Travail (Offres v2 OAuth2)          |
 |          Région Île-de-France (11) + Dept Lille (59)     |
 +---------------------------------------------------------+
                              |
                              v
 [ 🥉 BRONZE : JSON Brut ]
    extract_jobs.py -> data/bronze/ (Deduplication par id_offre)
                              |
                              v
 [ 🥈 SILVER : Processing Big Data & NLP Skills Extraction ]
    transform_silver.py -> data/silver/
    - Moteur PySpark (Local master[*])
    - Filtrage : Alternance 24 Mois uniquement & Entreprises Directes
    - Regex NLP : Extraction de 24+ technos
    - Construction de l'URL de postulation directe
                              |
                              v
 [ 🥇 GOLD : Data Warehouse DuckDB & Modélisation dbt ]
    load_gold.py + dbt_project/ -> data/job_market.db
    - Ingestion DuckDB (UPSERT sur id_offre)
    - dbt Star Schema : stg_offres -> fact_offres & dim_*
    - Data Quality Tests (6/6 PASSED)
                              |
         +--------------------+--------------------+
         |                                         |
         v                                         v
  [ 📱 TELEGRAM BOT ]                   [ 📊 DASHBOARD STREAMLIT ]
  Alertes Push directes                 Interface Web interactive
  avec lien de postulation              http://localhost:8501
```

---

## 🛠️ Stack Technique & Les 5 Piliers

| Pilier | Technologie | Rôle dans le Projet |
| :--- | :--- | :--- |
| **1. Extraction & Ingestion** | **Python, Requests, OAuth2** | Authentification sécurisée et extraction paginée multi-zones depuis l'API France Travail. |
| **2. Processing & Big Data** | **PySpark 4.2 (Spark Core/SQL)** | Processing distribué dans la couche Silver, filtrage regex des écoles, extraction NLP des compétences tech. |
| **3. Data Warehouse & Modeling** | **DuckDB 1.0 + dbt-duckdb 1.8+** | Stockage colonne analytique rapide + Modélisation dimensionnelle en étoile (Fact & Dimensions). |
| **4. Orchestration & DevOps** | **Apache Airflow 2.10 + Docker** | Conteneurisation (PostgreSQL, Webserver, Scheduler, Init) avec image custom Java OpenJDK 17 + PySpark. |
| **5. Restitution & Alerting** | **Streamlit + Telegram Bot** | Application Web interactive (Plotly) + Notifications push instantanées avec liens directs. |

---

## ⭐️ Modélisation en Étoile (dbt Star Schema)

Le projet utilise **dbt (data build tool)** pour transformer la table nettoyée Silver en un schéma dimensionnel en étoile optimisé pour les requêtes analytiques :

```text
               +----------------------+
               |    dim_entreprises   |
               +----------------------+
               | id_entreprise (PK)   |
               | nom_entreprise       |
               +----------------------+
                          ^
                          |
+---------------------+   |   +-----------------------+
|  dim_localisations  |   |   |    dim_competences    |
+---------------------+   |   +-----------------------+
| id_localisation (PK)|---+---| id_competence (PK)    |
| localisation        |   |   | nom_competence        |
| zone_geographique   |   |   +-----------------------+
+---------------------+   |               ^
                          |               |
               +----------------------+   |
               |      fact_offres     |---|
               +----------------------+
               | id_offre (PK)        |
               | id_entreprise (FK)   |
               | id_localisation (FK) |
               | titre_poste          |
               | type_contrat         |
               | duree_contrat        |
               | competences_tech     |
               | url_offre            |
               | date_publication     |
               +----------------------+
```

---

## 📂 Structure du Dépôt

```text
job-market-data-pipeline/
├── dags/
│   └── job_market_dag.py         # DAG Apache Airflow (Schedule quotidien 08:00 UTC)
├── dbt_project/
│   ├── dbt_project.yml           # Configuration du projet dbt
│   ├── profiles.yml              # Profil de connexion dbt -> DuckDB
│   └── models/
│       ├── staging/
│       │   ├── src_duckdb.yml    # Déclaration des sources DuckDB
│       │   └── stg_offres.sql    # Vue Staging nettoyée
│       └── marts/
│           ├── schema.yml        # Tests de qualité (unique, not_null)
│           ├── dim_entreprises.sql
│           ├── dim_localisations.sql
│           ├── dim_competences.sql
│           └── fact_offres.sql   # Table de faits centrale
├── data/                         # Stockage des couches Medallion (gitignore)
│   ├── bronze/                   # Fichiers bruts JSON
│   ├── silver/                   # Fichiers nettoyés CSV
│   └── job_market.db             # Base de données analytique DuckDB
├── app.py                        # Application Web & Dashboard Streamlit
├── extract_jobs.py               # Module Bronze : Extraction API
├── transform_silver.py           # Module Silver : Processing PySpark NLP
├── load_gold.py                  # Module Gold : Ingestion DuckDB & dbt
├── notifier.py                   # Engine de notification Telegram & Discord
├── run_pipeline.py               # Orchestrateur Python local
├── Dockerfile.airflow            # Image Airflow custom (Java JRE 17 + PySpark)
├── docker-compose.yml            # Stack Docker (Airflow Webserver/Scheduler/PostgreSQL)
├── requirements.txt              # Dépendances Python du projet
└── README.md                     # Documentation officielle
```

---

## 🚀 Guide d'Installation & d'Exécution

### 1. Cloner le projet et configurer l'environnement local
```bash
git clone https://github.com/asensio06/job-market-data-pipeline.git
cd job-market-data-pipeline

# Créer et activer l'environnement virtuel Python
python3 -m venv venv
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

### 2. Configurer les variables d'environnement (`.env`)
Créez un fichier `.env` à la racine du projet avec vos identifiants :
```env
FRANCETRAVAIL_CLIENT_ID="votre_client_id"
FRANCETRAVAIL_CLIENT_SECRET="votre_client_secret"
TELEGRAM_BOT_TOKEN="votre_bot_token"
TELEGRAM_CHAT_ID="votre_chat_id"
```

---

### 3. Exécution du Pipeline en Mode Local
Pour exécuter le pipeline Medallion complet immédiatement :
```bash
python run_pipeline.py
```

---

### 4. Lancement de l'Orchestrateur Apache Airflow (Docker)
Pour faire tourner le pipeline dans un environnement conteneurisé professionnel :

```bash
# Reconstruire l'image et démarrer les conteneurs
docker compose build
docker compose up -d
```

- 🌐 **Interface Airflow** : [http://localhost:8080](http://localhost:8080)
- **Identifiants** : `admin` / `admin`

---

### 5. Lancement du Dashboard Streamlit
Pour ouvrir l'interface Web interactive et explorer les offres :

```bash
streamlit run app.py
```

- 🌐 **Interface Dashboard** : [http://localhost:8501](http://localhost:8501)

---

## 📊 Visualisation & Alertes

### 📱 Notification Push Telegram
Dès qu'une nouvelle alternance de 24 mois est détectée, une alerte est envoyée sur Telegram avec le lien direct de postulation :

```text
🚨 NOUVELLE OFFRE DATA (24 MOIS) 🚨

📌 Poste : Apprenti Data Ingénieur H/F
🏢 Entreprise : SPIE Groupe
📍 Lieu : 95 - Cergy
📜 Contrat : CDD - 24 Mois
💻 Skills : python, sql, spark, airflow, docker, git
🔑 ID Offre : 5352664

🔗 Postuler directement :
https://candidat.francetravail.fr/offres/recherche/detail/5352664
```

### 🌐 Dashboard Web Streamlit
- **KPIs en direct** (Offres 24 mois, Entreprises, Top skill).
- **Graphiques Plotly** (Top 10 des technos demandées, Répartition géographique IDF vs Lille).
- **Table d'offres filtrables** avec colonne de lien direct **Postuler 🚀**.

---

## 🧪 Qualité des Données & Tests

Le projet intègre une suite de Data Quality Tests automatisée via **dbt** :

```bash
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

- ✅ `not_null_fact_offres_id_offre` (PASSED)
- ✅ `unique_fact_offres_id_offre` (PASSED)
- ✅ `not_null_dim_entreprises_id_entreprise` (PASSED)
- ✅ `unique_dim_entreprises_id_entreprise` (PASSED)
- ✅ `not_null_dim_competences_id_competence` (PASSED)
- ✅ `unique_dim_competences_id_competence` (PASSED)

---

## 👨‍💻 Auteur
**Arsène Fogué** — *Data Engineer / Analytics Engineer*  
- 🐙 GitHub : [@asensio06](https://github.com/asensio06)  
