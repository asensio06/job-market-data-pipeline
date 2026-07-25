# 📖 Manuel d'Utilisation Complet — Job Market Data Pipeline

Bienvenue dans le manuel d'utilisation de la plateforme de données **Job Market Data Pipeline**. Ce document explique en détail comment utiliser, exécuter, superviser et tester le pipeline End-to-End.

---

## 📑 Sommaire
1. [Vue d'ensemble du Projet](#1-vue-densemble-du-projet)
2. [Pré-requis & Configuration Initiales](#2-pré-requis--configuration-initiales)
3. [Mode 1 : Orchestration avec Apache Airflow & Docker (Recommandé)](#3-mode-1--orchestration-avec-apache-airflow--docker-recommandé)
4. [Mode 2 : Exécution Manuelle en Ligne de Commande](#4-mode-2--exécution-manuelle-en-ligne-de-commande)
5. [Mode 3 : Visualisation Web (Dashboard Streamlit)](#5-mode-3--visualisation-web-dashboard-streamlit)
6. [Mode 4 : Tests de Qualité des Données (dbt)](#6-mode-4--tests-de-qualité-des-données-dbt)
7. [Alertes Push Telegram](#7-alertes-push-telegram)
8. [Résolution des Problèmes Courants](#8-résolution-des-problèmes-courants)

---

## 1. Vue d'ensemble du Projet

Le projet collecte, nettoie, stocke et analyse les offres d'emploi et d'alternance en Data / IA sur les zones **Île-de-France** et **Lille / Nord**.

### Architecture Medallion :
- **Bronze** : Extraction brute JSON depuis l'API France Travail (OAuth2).
- **Silver** : Processing **PySpark** pour filtrer les alternances de **24 mois uniquement** (hors écoles) et extraire les compétences tech via regex NLP.
- **Gold** : Ingestion dans **DuckDB**, modélisation en étoile **dbt** (`fact_offres`, `dim_entreprises`, `dim_localisations`, `dim_competences`) et vues analytiques.

---

## 2. Pré-requis & Configuration Initiales

### 1. Cloner le projet et créer le virtualenv Python
```bash
cd /home/arsene/job-market-data-pipeline
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurer les clés API (`.env`)
Assurez-vous que le fichier `.env` à la racine contient :
```env
FRANCETRAVAIL_CLIENT_ID="votre_client_id"
FRANCETRAVAIL_CLIENT_SECRET="votre_client_secret"
TELEGRAM_BOT_TOKEN="8948734269:AAEaRV2jNV8Rusu-Jg_4dD5LekfFfSheknY"
TELEGRAM_CHAT_ID="1839161044"
```

---

## 3. Mode 1 : Orchestration avec Apache Airflow & Docker (Recommandé)

Ce mode permet de faire tourner le pipeline de façon 100% automatique et conteneurisée.

### Démarrer le cluster Docker Airflow
```bash
docker compose up -d
```

### Accéder à l'interface d'administration Airflow
- **URL** : [http://localhost:8080](http://localhost:8080)
- **Utilisateur** : `admin`
- **Mot de passe** : `admin`

### Superviser et déclencher le DAG
1. Repérez le DAG **`job_market_medallion_pipeline`**.
2. Pour un lancement immédiat, cliquez sur le bouton **Play (▶️)** ➔ **Trigger DAG**.
3. **Planification automatique** : Le DAG s'exécute automatiquement **tous les jours à 08h00 UTC**.

---

## 4. Mode 2 : Exécution Manuelle en Ligne de Commande

Si vous souhaitez exécuter le pipeline immédiatement en local sans passer par Airflow :

```bash
python run_pipeline.py
```

### Ce qui s'exécute automatiquement :
1. `extract_jobs.py` : Télécharge les offres brutes JSON dans `data/bronze/`.
2. `transform_silver.py` : Lance le moteur **PySpark** et exporte le CSV nettoyé dans `data/silver/`.
3. `load_gold.py` : Ingeste les données dans **DuckDB**, lance `dbt run` & `dbt test`, et envoie une alerte Telegram s'il y a de nouvelles offres.

---

## 5. Mode 3 : Visualisation Web (Dashboard Streamlit)

Pour explorer visuellement les offres et les graphiques analytiques :

### Lancer l'application Streamlit
```bash
streamlit run app.py
```

### Naviguer sur le Dashboard
- Ouvrez votre navigateur sur : **[http://localhost:8501](http://localhost:8501)**
- **Fonctionnalités** :
  - **Metrics** : Nombre d'offres ciblées (24 mois), top compétence demandée, nombre d'entreprises.
  - **Graphique Plotly** : Histogramme du Top 10 des compétences Tech.
  - **Filtres interactifs** : Filtrage par région, par entreprise et recherche par mot-clé.

---

## 6. Mode 4 : Tests de Qualité des Données (dbt)

Pour vérifier l'intégrité des modèles dimensionnels en étoile et la qualité des données :

### Exécuter la modélisation dbt (Star Schema)
```bash
dbt run --project-dir dbt_project --profiles-dir dbt_project
```

### Exécuter les 6 tests de qualité dbt
```bash
dbt test --project-dir dbt_project --profiles-dir dbt_project
```
- **Tests vérifiés** : Unicité (`unique`) et non-nullité (`not_null`) des clés primaires sur `fact_offres`, `dim_entreprises` et `dim_competences`.

---

## 7. Alertes Push Telegram

Chaque fois que de nouvelles offres d'alternance 24 mois sont détectées, le bot Telegram `@asensio123bot` vous envoie directement une notification push contenant :
- L'intitulé du poste
- Le nom de l'entreprise
- La ville
- La liste des compétences recherchées
- Le lien direct vers l'offre

---

## 8. Résolution des Problèmes Courants

| Symptôme | Cause probable | Solution |
| :--- | :--- | :--- |
| **`PermissionError: data/...` dans Docker** | Les conteneurs Airflow s'exécutent avec un mauvais UID. | Le fichier `docker-compose.yml` est configuré avec `user: "1000:0"`. Relancez `docker compose up -d`. |
| **`PySparkImportError: Pandas >= 2.2.0`** | Version de Pandas obsolète dans le conteneur. | L'image custom `Dockerfile.airflow` reconstruit l'environnement avec `pandas>=2.2.0`. Exécutez `docker compose build`. |
| **Erreur de connexion DuckDB (base verrouillée)** | DuckDB ne permet qu'un seul écrivain à la fois. | Fermez l'application Streamlit ou toute autre session Python accédant en écriture à `data/job_market.db`. |
