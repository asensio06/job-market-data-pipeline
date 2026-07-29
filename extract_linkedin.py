import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "linkedin-data-api.p.rapidapi.com")

def extract_linkedin_jobs():
    """
    Extrait les offres d'emploi LinkedIn depuis RapidAPI et les sauvegarde dans data/bronze/.
    """
    print("\n--- 🔍 EXTRACTION LINKEDIN (RapidAPI) ---")
    
    if not RAPIDAPI_KEY:
        print("⚠️ RAPIDAPI_KEY n'est pas configurée dans le fichier .env.")
        print("💡 Pour activer l'extraction LinkedIn, ajoutez RAPIDAPI_KEY=\"votre_clé_rapidapi\" dans .env.")
        return None

    url = f"https://{RAPIDAPI_HOST}/search-jobs"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST
    }

    all_jobs = []
    queries = [
        {"keywords": "Data Alternance", "location": "Paris, Île-de-France, France"},
        {"keywords": "Data Alternance", "location": "Lille, Hauts-de-France, France"}
    ]

    for q in queries:
        print(f"🔍 Recherche LinkedIn : {q['keywords']} à {q['location']}...")
        params = {
            "keywords": q["keywords"],
            "locationId": q["location"],
            "datePosted": "pastMonth",
            "sort": "mostRecent"
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                jobs = data.get("data", data.get("items", [])) if isinstance(data, dict) else data
                if isinstance(jobs, list):
                    all_jobs.extend(jobs)
                    print(f"  ✅ {len(jobs)} offres LinkedIn récupérées.")
                else:
                    print(f"  ⚠️ Réponse inattendue : {data}")
            else:
                print(f"  ⚠️ Erreur API LinkedIn ({response.status_code}) : {response.text[:200]}")
        except Exception as e:
            print(f"  ❌ Exception lors de l'extraction LinkedIn : {e}")

    if not all_jobs:
        print("ℹ️ Aucune offre LinkedIn n'a été extraite.")
        return None

    # Déduplication par ID si disponible
    unique_jobs = {}
    for job in all_jobs:
        job_id = str(job.get("id", job.get("jobId", job.get("url", ""))))
        if job_id:
            unique_jobs[job_id] = job

    os.makedirs("data/bronze", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"data/bronze/linkedin_offres_{timestamp}.json"
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(list(unique_jobs.values()), f, ensure_ascii=False, indent=2)

    print(f"💾 {len(unique_jobs)} offres uniques LinkedIn sauvegardées dans : {filepath}")
    return filepath

if __name__ == "__main__":
    extract_linkedin_jobs()
