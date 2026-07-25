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
    
    # Étape 1 : COUCHE BRONZE (Extraction API)
    print("\n--- 1. ÉTAPE BRONZE (EXTRACTION API) ---")
    token = get_access_token()
    if not token:
        print("❌ Échec lors de la récupération du token API. Fin du pipeline.")
        sys.exit(1)
        
    job_data = fetch_job_offers(token)
    if not job_data or not job_data.get("resultats"):
        print("⚠️ Aucune donnée récupérée depuis l'API. Arrêt du pipeline.")
        sys.exit(1)
        
    save_to_bronze(job_data)
    
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
