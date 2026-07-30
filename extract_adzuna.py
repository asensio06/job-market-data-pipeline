import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

def extract_adzuna_jobs():
    """
    Extrait les offres d'emploi depuis l'API officielle Adzuna France et les sauvegarde dans data/bronze/.
    """
    print("\n--- 🔍 EXTRACTION ADZUNA (API Officielle France) ---")
    
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("⚠️ ADZUNA_APP_ID ou ADZUNA_APP_KEY n'est pas configuré dans le fichier .env.")
        print("💡 Obtenez gratuitement vos clés en 1 minute sur https://developer.adzuna.com et ajoutez-les dans .env.")
        return None

    all_jobs = []
    queries = [
        {"keywords": "Data Alternance", "where": "Paris"},
        {"keywords": "Data Alternance", "where": "Lille"}
    ]

    for q in queries:
        url = f"https://api.adzuna.com/v1/api/jobs/fr/search/1"
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "what": q["keywords"],
            "where": q["where"],
            "results_per_page": 50,
            "content-type": "application/json"
        }
        print(f"🔍 Recherche Adzuna : '{q['keywords']}' à {q['where']}...")
        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if isinstance(results, list):
                    all_jobs.extend(results)
                    print(f"  ✅ {len(results)} offres Adzuna récupérées pour {q['where']}.")
                else:
                    print(f"  ⚠️ Réponse inattendue : {data}")
            else:
                print(f"  ⚠️ Erreur API Adzuna ({response.status_code}) : {response.text[:200]}")
        except Exception as e:
            print(f"  ❌ Exception lors de l'extraction Adzuna : {e}")

    if not all_jobs:
        print("ℹ️ Aucune offre Adzuna n'a été extraite.")
        return None

    # Déduplication par ID Adzuna
    unique_jobs = {}
    for job in all_jobs:
        job_id = str(job.get("id", job.get("redirect_url", "")))
        if job_id:
            unique_jobs[job_id] = job

    os.makedirs("data/bronze", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"data/bronze/adzuna_offres_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(list(unique_jobs.values()), f, ensure_ascii=False, indent=2)

    print(f"💾 {len(unique_jobs)} offres uniques Adzuna sauvegardées dans : {filepath}")
    return filepath

if __name__ == "__main__":
    extract_adzuna_jobs()
