import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

CLIENT_ID = os.getenv("FRANCETRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCETRAVAIL_CLIENT_SECRET")

# URLs de l'API France Travail
AUTH_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
API_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

def get_access_token():
    """Récupère le token d'accès OAuth2 de France Travail."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "api_offresdemploiv2 o2dsoffre"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    response = requests.post(AUTH_URL, data=payload, headers=headers)
    
    if response.status_code == 200:
        print("✅ Token d'accès récupéré avec succès.")
        return response.json().get("access_token")
    else:
        print(f"❌ Erreur d'authentification ({response.status_code}): {response.text}")
        return None

def fetch_job_offers(token):
    """Récupère les offres d'emploi 'Data' en Île-de-France."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    # Paramètres : mot-clé "data" et région Île-de-France (code 11)
    params = {
        "motsCles": "data",
        "region": "11"
    }
    
    response = requests.get(API_URL, headers=headers, params=params)
    
    if response.status_code in (200, 206):
        data = response.json()
        offres = data.get("resultats", [])
        print(f"✅ Extraction réussie : {len(offres)} offres récupérées (Code {response.status_code}).")
        return data
    elif response.status_code == 204:
        print("⚠️ Aucune offre trouvée avec ces critères.")
        return None
    else:
        print(f"❌ Erreur lors de l'extraction ({response.status_code}): {response.text}")
        return None

def save_to_bronze(data):
    """Sauvegarde les données brutes dans la couche Bronze avec la date et l'heure."""
    # Formatage de la date (ex: 20260718_193000)
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/bronze/offres_idf_data_{today}.json"
    
    # S'assurer que les dossiers parents existent
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Écriture du fichier JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print(f"💾 Données sauvegardées avec succès dans : {filename}")

if __name__ == "__main__":
    print("🚀 Démarrage du pipeline d'extraction...")
    
    # 1. Authentification
    access_token = get_access_token()
    
    if access_token:
        # 2. Extraction des données
        job_data = fetch_job_offers(access_token)
        
        if job_data:
            # 3. Sauvegarde dans la couche Bronze
            save_to_bronze(job_data)
            
    print("🏁 Fin du script.")