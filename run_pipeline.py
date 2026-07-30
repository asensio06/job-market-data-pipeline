"""
Pipeline d'Ingénierie de Données - Marché de l'Emploi Data (IDF & Lille)
Orchestrateur Principal (Bronze -> Silver -> Gold)
"""

import sys
from extract_jobs import get_access_token, fetch_job_offers, save_to_bronze
from transform_silver import process_silver_layer
from load_gold import process_gold_layer

def run_full_pipeline():
    print("=" * 60)
    print("🚀 LANCEMENT DU PIPELINE MEDALLION COMPLET (Bronze -> Silver -> Gold)")
    print("=" * 60)
    
    # Étape 1 : COUCHE BRONZE (Extraction APIs Multi-Sources)
    print("\n--- 1. ÉTAPE BRONZE (EXTRACTION APIS MULTI-SOURCES) ---")
    token = get_access_token()
    if token:
        job_data = fetch_job_offers(token)
        if job_data and job_data.get("resultats"):
            save_to_bronze(job_data)

    # Extraction optionnelle LinkedIn (RapidAPI)
    try:
        from extract_linkedin import extract_linkedin_jobs
        extract_linkedin_jobs()
    except Exception as e:
        print(f"ℹ️ Étape LinkedIn ignorée : {e}")

    # Extraction optionnelle Adzuna (API Officielle)
    try:
        from extract_adzuna import extract_adzuna_jobs
        extract_adzuna_jobs()
    except Exception as e:
        print(f"ℹ️ Étape Adzuna ignorée : {e}")
    
    # Étape 2 : COUCHE SILVER (Nettoyage & Filtrage)
    print("\n--- 2. ÉTAPE SILVER (TRANSFORMATION & FILTRAGE) ---")
    process_silver_layer()
    
    # Étape 3 : COUCHE GOLD (Base de Données & Vues Analytiques)
    print("\n--- 3. ÉTAPE GOLD (DUCKDB & REPORTING) ---")
    process_gold_layer()
    
    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXÉCUTÉ AVEC SUCCÈS !")
    print("=" * 60)

if __name__ == "__main__":
    run_full_pipeline()
