"""
DAG Apache Airflow - Job Market Data Pipeline (Medallion Architecture)
Bronze (Extract) -> Silver (Transform) -> Gold (DuckDB & Telegram Notifier)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ajouter le répertoire racine au PATH pour importer nos modules
sys.path.append('/opt/airflow')

# Import de nos modules personnalisés
from extract_jobs import get_access_token, fetch_job_offers, save_to_bronze
from transform_silver import process_silver_layer
from load_gold import process_gold_layer

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_bronze_task():
    """Tâche Airflow 1 : Extraction Multi-Sources (France Travail, LinkedIn, Adzuna) -> Bronze JSON"""
    print("🚀 Execution de la tache Bronze (Extraction APIs Multi-Sources)...")
    try:
        token = get_access_token()
        if token:
            job_data = fetch_job_offers(token)
            if job_data and job_data.get("resultats"):
                save_to_bronze(job_data)
    except Exception as e:
        print(f"⚠️ Extraction France Travail : {e}")

    try:
        from extract_linkedin import extract_linkedin_jobs
        extract_linkedin_jobs()
    except Exception as e:
        print(f"⚠️ Extraction LinkedIn : {e}")

    try:
        from extract_adzuna import extract_adzuna_jobs
        extract_adzuna_jobs()
    except Exception as e:
        print(f"⚠️ Extraction Adzuna : {e}")

    print("✅ Tache Bronze terminee avec succes.")

def transform_silver_task():
    """Tâche Airflow 2 : Nettoyage et filtrage -> Silver CSV"""
    print("🛠️ Execution de la tache Silver (Transformation & Filtrage)...")
    process_silver_layer()
    print("✅ Tache Silver terminee avec succes.")

def load_gold_task():
    """Tâche Airflow 3 : Ingestion DuckDB & Alerte Telegram -> Gold"""
    print("🏆 Execution de la tache Gold (DuckDB & Reporting/Alerte)...")
    process_gold_layer()
    print("✅ Tache Gold terminee avec succes.")

# Définition du DAG Airflow
with DAG(
    dag_id='job_market_medallion_pipeline',
    default_args=default_args,
    description='Pipeline ETL de suivi des alternances Data 24 mois (IDF & Lille)',
    schedule_interval='0 8 * * *',  # Exécution quotidienne à 8h00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data_engineering', 'medallion', 'duckdb', 'telegram'],
) as dag:

    # 1. Tâche Bronze
    t1_bronze = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze_task,
    )

    # 2. Tâche Silver
    t2_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver_task,
    )

    # 3. Tâche Gold
    t3_gold = PythonOperator(
        task_id='load_gold',
        python_callable=load_gold_task,
    )

    # Dépendances du DAG (Ordre d'exécution)
    t1_bronze >> t2_silver >> t3_gold
