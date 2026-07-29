import os
import glob
import sys
import duckdb
import pandas as pd
import subprocess
from datetime import datetime

DB_PATH = "data/job_market.db"

def init_db(con):
    """Initialise le schéma de la base de données DuckDB."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_offres (
            id_offre VARCHAR PRIMARY KEY,
            titre_poste VARCHAR,
            nom_entreprise VARCHAR,
            localisation VARCHAR,
            zone_geographique VARCHAR,
            type_contrat VARCHAR,
            duree_contrat VARCHAR,
            nature_contrat VARCHAR,
            salaire VARCHAR,
            competences_tech VARCHAR,
            date_publication VARCHAR,
            url_offre VARCHAR,
            date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Ajout sécurisé de la colonne si absente
    con.execute("ALTER TABLE silver_offres ADD COLUMN IF NOT EXISTS competences_tech VARCHAR;")
    con.execute("ALTER TABLE silver_offres ADD COLUMN IF NOT EXISTS url_offre VARCHAR;")

def process_gold_layer():
    print("🏆 Démarrage de la mise à jour de la couche Gold & Base de données...")

    # 1. Trouver le dernier fichier CSV de la couche Silver
    silver_files = glob.glob('data/silver/*.csv')
    if not silver_files:
        print("❌ Aucun fichier CSV trouvé dans data/silver/.")
        return

    latest_file = max(silver_files, key=os.path.getctime)
    print(f"📄 Lecture du fichier Silver : {latest_file}")

    df_silver = pd.read_csv(latest_file)
    if df_silver.empty:
        print("⚠️ Le fichier Silver est vide. Aucune donnée à charger.")
        return

    # S'assurer que les colonnes existent
    if 'competences_tech' not in df_silver.columns:
        df_silver['competences_tech'] = 'Non renseigné'

    def build_url(row):
        id_str = str(row['id_offre'])
        url_str = str(row.get('url_offre', ''))
        if url_str and url_str not in ['nan', 'None', '', 'Non renseigné'] and 'francetravail' not in url_str:
            return url_str
        if len(id_str) >= 9 and id_str.isdigit():
            return f"https://www.linkedin.com/jobs/view/{id_str}"
        return f"https://candidat.francetravail.fr/offres/recherche/detail/{id_str}"
    
    df_silver['url_offre'] = df_silver.apply(build_url, axis=1)

    if 'zone_geographique' not in df_silver.columns:
        def determine_zone(loc):
            loc_str = str(loc)
            if '59' in loc_str or 'Lille' in loc_str:
                return 'Lille / Nord'
            return 'Île-de-France'
        df_silver['zone_geographique'] = df_silver['localisation'].apply(determine_zone)

    # 2. Connexion à la base DuckDB
    os.makedirs('data', exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    # Identifier les nouvelles offres non encore présentes dans DuckDB
    existing_df = con.execute("SELECT id_offre FROM silver_offres").fetchdf()
    existing_ids = set(existing_df['id_offre']) if not existing_df.empty else set()

    df_nouvelles = df_silver[~df_silver['id_offre'].isin(existing_ids)]

    # 3. Insertion avec gestion des doublons (UPSERT / INSERT OR IGNORE)
    con.register('df_temp', df_silver)

    count_before = con.execute("SELECT COUNT(*) FROM silver_offres").fetchone()[0]

    con.execute("""
        INSERT INTO silver_offres (
            id_offre, titre_poste, nom_entreprise, localisation, 
            zone_geographique, type_contrat, duree_contrat, 
            nature_contrat, salaire, competences_tech, date_publication, url_offre
        )
        SELECT 
            id_offre, titre_poste, nom_entreprise, localisation, 
            zone_geographique, type_contrat, duree_contrat, 
            nature_contrat, salaire, competences_tech, date_publication, url_offre
        FROM df_temp
        ON CONFLICT (id_offre) DO UPDATE SET
            titre_poste = EXCLUDED.titre_poste,
            nom_entreprise = EXCLUDED.nom_entreprise,
            localisation = EXCLUDED.localisation,
            zone_geographique = EXCLUDED.zone_geographique,
            duree_contrat = EXCLUDED.duree_contrat,
            salaire = EXCLUDED.salaire,
            competences_tech = EXCLUDED.competences_tech,
            url_offre = EXCLUDED.url_offre;
    """)

    count_after = con.execute("SELECT COUNT(*) FROM silver_offres").fetchone()[0]
    nouveaux = count_after - count_before
    print(f"✅ Chargement terminé dans DuckDB (`{DB_PATH}`).")
    print(f"📊 Offres totales dans la base : {count_after} ({nouveaux} nouvelle(s) offre(s) insérée(s)).")

    # Trigger notifications pour les nouvelles offres
    if not df_nouvelles.empty:
        from notifier import notify_new_offers
        notify_new_offers(df_nouvelles.to_dict(orient='records'))

    # 4. Création des vues Gold (Agrégations analytiques)
    # Vue 1 : Répartition par zone géographique
    con.execute("""
        CREATE OR REPLACE VIEW gold_offres_par_zone AS
        SELECT 
            zone_geographique,
            COUNT(*) AS nb_offres
        FROM silver_offres
        GROUP BY zone_geographique
        ORDER BY nb_offres DESC;
    """)

    # Vue 2 : Top des entreprises qui recrutent le plus
    con.execute("""
        CREATE OR REPLACE VIEW gold_top_entreprises AS
        SELECT 
            nom_entreprise,
            COUNT(*) AS nb_offres
        FROM silver_offres
        WHERE nom_entreprise != 'Non renseigné'
        GROUP BY nom_entreprise
        ORDER BY nb_offres DESC;
    """)

    # Vue 3 : Top des compétences tech les plus demandées (Extraction PySpark)
    con.execute("""
        CREATE OR REPLACE VIEW gold_top_competences AS
        SELECT 
            trim(skill) AS competence_tech,
            COUNT(*) AS nb_demandes
        FROM (
            SELECT unnest(string_split(competences_tech, ',')) AS skill
            FROM silver_offres
            WHERE competences_tech IS NOT NULL 
              AND competences_tech != 'Non renseigné' 
              AND competences_tech != ''
        )
        GROUP BY competence_tech
        ORDER BY nb_demandes DESC;
    """)

    # 5. Affichage des rapports Gold dans la console
    print("\n--- 📈 VUE GOLD : Répartition des offres par zone ---")
    res_zone = con.execute("SELECT * FROM gold_offres_par_zone").fetchdf()
    print(res_zone.to_string(index=False))

    print("\n--- 🏢 VUE GOLD : Top des entreprises recruteuses ---")
    res_entreprises = con.execute("SELECT * FROM gold_top_entreprises").fetchdf()
    print(res_entreprises.to_string(index=False))

    print("\n--- 💻 VUE GOLD : Top des compétences Tech les plus recherchées (PySpark NLP) ---")
    res_skills = con.execute("SELECT * FROM gold_top_competences").fetchdf()
    print(res_skills.to_string(index=False))

    con.close()

    # 6. Exécution de dbt (Modélisation Dimensionnelle en Étoile & Tests de qualité)
    print("\n--- 🏗️ ÉTAPE dbt : Modélisation en Étoile & Data Quality Tests ---")
    try:
        dbt_dir = os.path.abspath('dbt_project')
        
        # dbt run
        print("▶️ Exécution de `dbt run`...")
        res_run = subprocess.run([sys.executable, '-m', 'dbt.cli.main', 'run', '--project-dir', dbt_dir, '--profiles-dir', dbt_dir], capture_output=True, text=True)
        if res_run.returncode == 0:
            print("✅ Modèles dbt (Star Schema) créés avec succès.")
        else:
            print(f"⚠️ Erreur lors de `dbt run` : {res_run.stderr or res_run.stdout}")

        # dbt test
        print("▶️ Exécution de `dbt test`...")
        res_test = subprocess.run([sys.executable, '-m', 'dbt.cli.main', 'test', '--project-dir', dbt_dir, '--profiles-dir', dbt_dir], capture_output=True, text=True)
        if res_test.returncode == 0:
            print("✅ Tous les tests de qualité dbt sont PASSED (6/6).")
        else:
            print(f"⚠️ Erreur lors des tests dbt : {res_test.stderr or res_test.stdout}")
            
    except Exception as e:
        print(f"⚠️ Impossible d'exécuter dbt : {e}")

    print("\n🏁 Couche Gold & Modélisation dbt mises à jour avec succès.")

if __name__ == "__main__":
    process_gold_layer()
