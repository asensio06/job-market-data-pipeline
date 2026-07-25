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
    """Récupère les offres d'emploi 'Data' en Île-de-France (Région 11) et Lille/Nord (Département 59)."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    zones = [
        {"nom": "Île-de-France", "params": {"motsCles": "data", "region": "11"}},
        {"nom": "Lille / Nord", "params": {"motsCles": "data", "departement": "59"}}
    ]
    
    toutes_offres_dict = {}
    
    for zone in zones:
        print(f"🔍 Extraction des offres pour : {zone['nom']}...")
        response = requests.get(API_URL, headers=headers, params=zone["params"])
        
        if response.status_code in (200, 206):
            data = response.json()
            offres = data.get("resultats", [])
            print(f"  ✅ {len(offres)} offres récupérées pour {zone['nom']} (Code {response.status_code}).")
            for offre in offres:
                toutes_offres_dict[offre["id"]] = offre
        elif response.status_code == 204:
            print(f"  ⚠️ Aucune offre trouvée pour {zone['nom']}.")
        else:
            print(f"  ❌ Erreur lors de l'extraction pour {zone['nom']} ({response.status_code}): {response.text}")
            
    offres_combinees = list(toutes_offres_dict.values())
    print(f"📊 Total des offres uniques extraites (IDF + Lille) : {len(offres_combinees)}")
    return {"resultats": offres_combinees}

def save_to_bronze(data):
    """Sauvegarde les données brutes dans la couche Bronze avec la date et l'heure."""
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/bronze/offres_idf_lille_data_{today}.json"
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print(f"💾 Données sauvegardées avec succès dans : {filename}")

if __name__ == "__main__":
    print("🚀 Démarrage du pipeline d'extraction (Île-de-France + Lille)...")
    
    # 1. Authentification
    access_token = get_access_token()
    
    if access_token:
        # 2. Extraction des données
        job_data = fetch_job_offers(access_token)
        
        if job_data and job_data.get("resultats"):
            # 3. Sauvegarde dans la couche Bronze
            save_to_bronze(job_data)
            
    print("🏁 Fin du script d'extraction.")