# 📊 Job Market Data Engineering Pipeline (Île-de-France & Lille)

Un pipeline analytique d'ingénierie de données **End-to-End (Bout-en-Bout)** construit selon l'architecture **Medallion (Bronze ➔ Silver ➔ Gold)** pour extraire, nettoyer avec **PySpark**, modéliser avec **dbt**, stocker dans **DuckDB**, orchestrer via **Apache Airflow** dans **Docker**, et visualiser via un **Dashboard Streamlit** & des alertes **Telegram**.

---

## 🏛️ Architecture du Projet

```text
 +----------------------------------+
 |  API France Travail (v2 Offres)  | (IDF Région 11 + Lille Dept 59)
 +----------------------------------+
                  |
                  v
 [ 🥉 BRONZE : JSON Brut ]
    extract_jobs.py -> data/bronze/
                  |
                  v
 [ 🥈 SILVER : PySpark Processing & NLP Skills Extraction ]
    transform_silver.py -> data/silver/
                  |
                  v
 [ 🥇 GOLD : DuckDB Data Warehouse & dbt Star Schema ]
    load_gold.py + dbt_project/ -> data/job_market.db
                  |
         +--------+--------+
         |                 |
         v                 v
  [ 📱 TELEGRAM BOT ]   [ 📊 DASHBOARD STREAMLIT ]
  Notifications Push    http://localhost:8501
```

---

## 🛠️ Stack Technique (5 Piliers Data Engineering)

1. **Extraction & API** : Python, OAuth2, API France Travail (Île-de-France & Lille).
2. **Big Data Processing (Silver)** : **PySpark** (Filtrage des alternances 24 mois, exclusion des écoles, extraction NLP des compétences tech).
3. **Data Warehouse & Modélisation (Gold)** : **DuckDB** + **dbt (data build tool)** (Schéma en étoile `fact_offres`, `dim_entreprises`, `dim_localisations`, `dim_competences` + 6 Data Quality Tests).
4. **Orchestration & DevOps** : **Apache Airflow 2.10.4** + **PostgreSQL** conteneurisés dans **Docker Compose**.
5. **Restitution & Alertes** : **Streamlit** (Dashboard Web interactif) + **Telegram Bot** (Alertes push en temps réel).

---

## 🚀 Guide d'Exécution

### 1. Cloner & Installer l'environnement local
```bash
git clone <votre_repo>
cd job-market-data-pipeline

# Activer le venv et installer les dépendances
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration des variables d'environnement (`.env`)
```env
FRANCETRAVAIL_CLIENT_ID="votre_client_id"
FRANCETRAVAIL_CLIENT_SECRET="votre_client_secret"

TELEGRAM_BOT_TOKEN="votre_bot_token"
TELEGRAM_CHAT_ID="votre_chat_id"
```

### 3. Exécuter le pipeline Medallion complet
```bash
python run_pipeline.py
```

### 4. Lancer le Cluster Apache Airflow (Docker)
```bash
docker compose build
docker compose up -d
```
- 🌐 **Interface Airflow** : [http://localhost:8080](http://localhost:8080) (Identifiants : `admin` / `admin`).

### 5. Lancer le Dashboard Web Streamlit
```bash
streamlit run app.py
```
- 🌐 **Interface Dashboard** : [http://localhost:8501](http://localhost:8501).

---

## 🧪 Tests de Qualité des Données (dbt)
Pour exécuter les 6 tests dbt de non-nullité et d'unicité sur les clés primaires :
```bash
dbt test --project-dir dbt_project --profiles-dir dbt_project
```
