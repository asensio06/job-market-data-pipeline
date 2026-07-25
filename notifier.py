import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def send_telegram_message(message):
    """Envoie un message via un bot Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
        
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        res = requests.post(url, json=payload, timeout=10)
        return res.status_code == 200
    except Exception as e:
        print(f"❌ Erreur d'envoi Telegram : {e}")
        return False

def send_discord_message(message):
    """Envoie un message via un Webhook Discord."""
    if not DISCORD_WEBHOOK_URL:
        return False
        
    payload = {"content": message}
    try:
        res = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        return res.status_code in (200, 204)
    except Exception as e:
        print(f"❌ Erreur d'envoi Discord : {e}")
        return False

def notify_new_offers(new_offers_list):
    """
    Prend une liste de dictionnaires d'offres et envoie une alerte pour chaque nouvelle offre.
    """
    if not new_offers_list:
        print("ℹ️ Aucune nouvelle offre à notifier.")
        return

    print(f"🔔 Préparation des alertes pour {len(new_offers_list)} nouvelle(s) offre(s)...")

    for offer in new_offers_list:
        titre = offer.get("titre_poste", "Non renseigné")
        entreprise = offer.get("nom_entreprise", "Non renseignée")
        lieu = offer.get("localisation", "Non renseignée")
        contrat = offer.get("duree_contrat", offer.get("type_contrat", "24 mois"))
        id_offre = offer.get("id_offre", "")
        url_offre = offer.get("url_offre", f"https://candidat.francetravail.fr/offres/recherche/detail/{id_offre}")
        competences = offer.get("competences_tech", "Non renseigné")

        msg = (
            f"🚨 *NOUVELLE OFFRE DATA (24 MOIS)* 🚨\n\n"
            f"📌 *Poste* : {titre}\n"
            f"🏢 *Entreprise* : {entreprise}\n"
            f"📍 *Lieu* : {lieu}\n"
            f"📜 *Contrat* : {contrat}\n"
            f"💻 *Skills* : {competences}\n"
            f"🔑 *ID Offre* : `{id_offre}`\n\n"
            f"🔗 *Postuler directement* :\n{url_offre}\n"
        )

        # 1. Console Log
        print(f"\n--- ALERTE NOUVELLE OFFRE [{id_offre}] ---")
        print(f"Poste: {titre}")
        print(f"Entreprise: {entreprise} ({lieu})")
        print(f"Contrat: {contrat}")

        # 2. Telegram (si configuré)
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            sent = send_telegram_message(msg)
            if sent:
                print(f"📱 Alerte Telegram envoyée pour l'offre {id_offre}.")

        # 3. Discord (si configuré)
        if DISCORD_WEBHOOK_URL:
            # Remplacement syntaxe Markdown simple pour Discord
            discord_msg = msg.replace("*", "**").replace("`", "`")
            sent = send_discord_message(discord_msg)
            if sent:
                print(f"💬 Alerte Discord envoyée pour l'offre {id_offre}.")

if __name__ == "__main__":
    # Test d'exemple
    sample_offer = [{
        "id_offre": "TEST_123",
        "titre_poste": "Alternant Data Analyst (Test)",
        "nom_entreprise": "Entreprise Test",
        "localisation": "59 - Lille",
        "duree_contrat": "CDD - 24 Mois"
    }]
    notify_new_offers(sample_offer)
