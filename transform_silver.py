import os
import glob
import json
import re
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LISTE_SKILLS_TECH = [
    'python', 'sql', 'spark', 'pyspark', 'airflow', 'docker', 'snowflake', 
    'bigquery', 'dbt', 'tableau', 'power bi', 'powerbi', 'excel', 'aws', 
    'gcp', 'azure', 'git', 'scala', 'kafka', 'r', 'java', 'c++', 'nosql', 'mongodb'
]

def process_silver_layer():
    print("🛠️ Démarrage de la transformation Silver avec PySpark...")

    # 1. Identifier tous les fichiers JSON récents dans Bronze
    bronze_files = glob.glob('data/bronze/*.json')
    if not bronze_files:
        print("❌ Aucun fichier trouvé dans data/bronze/.")
        return
    
    # Charger et unifier les fichiers bruts (France Travail + LinkedIn)
    offres_list = []
    for filepath in set(bronze_files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            items = data.get('resultats', data) if isinstance(data, dict) else data
            if isinstance(items, list):
                for item in items:
                    # Normalisation des clés LinkedIn & Adzuna vers la structure unifiée
                    if 'jobTitle' in item or 'title' in item or 'redirect_url' in item:
                        title_text = str(item.get('jobTitle', item.get('title', ''))).lower()
                        desc_text = str(item.get('description', item.get('snippet', ''))).lower()
                        full_text = f"{title_text} {desc_text}"

                        # Vérification stricte : l'offre doit mentionner alternance / apprentissage / professionnalisation
                        is_alternance = any(k in full_text for k in ['alternan', 'apprentissage', 'professionnalisation'])
                        is_cdi = 'cdi' in title_text and not is_alternance

                        # Extraction nom entreprise Adzuna / LinkedIn
                        comp = item.get('companyName', item.get('company', ''))
                        if isinstance(comp, dict):
                            comp = comp.get('display_name', '')

                        # Extraction localisation Adzuna / LinkedIn
                        loc = item.get('location', '')
                        if isinstance(loc, dict):
                            loc = loc.get('display_name', '')

                        item['intitule'] = item.get('jobTitle', item.get('title', ''))
                        item['entreprise.nom'] = str(comp)
                        item['lieuTravail.libelle'] = str(loc)
                        item['description'] = item.get('description', item.get('snippet', ''))
                        item['id'] = str(item.get('id', item.get('jobId', '')))
                        item['alternance'] = 'True' if (is_alternance and not is_cdi) else 'False'
                        item['typeContratLibelle'] = 'CDD - 24 Mois' if (is_alternance and not is_cdi) else 'Autre'
                        
                        redirect_url = item.get('url', item.get('redirect_url', item.get('link', '')))
                        if redirect_url:
                            item['url_offre'] = redirect_url
                    offres_list.append(item)
        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture de {filepath} : {e}")

    if not offres_list:
        print("⚠️ Aucune offre valide trouvée dans les fichiers JSON.")
        return

    # Pandas normalization pour garantir la propreté de la structure
    df_pd = pd.json_normalize(offres_list).astype(str)
    print(f"📊 Nombre total d'offres avant filtrage (Toutes sources) : {len(df_pd)}")

    # 2. Initialisation de PySpark
    spark = SparkSession.builder \
        .appName("JobMarketSilverPySpark") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Chargement dans PySpark DataFrame
    df_spark = spark.createDataFrame(df_pd)

    # 3. Filtrage 1 : Alternance uniquement
    if 'alternance' in df_spark.columns:
        df_spark = df_spark.filter(F.col('alternance') == 'True')
    elif 'natureContrat' in df_spark.columns:
        df_spark = df_spark.filter(F.col('natureContrat').rlike('(?i)apprentissage|professionnalisation'))

    # Exclusion stricte des postes CDI explicites dans le titre (ex: "Data Analyst - CDI")
    df_spark = df_spark.filter(~F.col('intitule').rlike('(?i)\\bCDI\\b'))

    # 4. Filtrage 2 : Intitulés de postes ciblés
    mots_cles_postes = '(?i)data analyst|data scientist|data engineer|qualité et de la performance|business analyst|data manager|data'
    if 'intitule' in df_spark.columns:
        df_spark = df_spark.filter(F.col('intitule').rlike(mots_cles_postes))

    # 5. Filtrage 3 : Exclure les écoles / organismes de formation / cabinets de recrutement
    regex_ecoles = '(?i)iscod|kaischool|openclassrooms|studi|pigier|epitech|cfa|ecole|école|campus|euridis|mbway|formation|imc|cesi|wild code school|ironhack|jedha|albert school|datascientest|efrei|esme|epita|ece|essec|hec|escp|skema|edhec|audencia|neoma|kedge|tbs|grenoble em|emlyon|igsub|esg|insee|isep|estaca|supinfo|devinci|iim|esd|digital school|ionis|afpa|greta|cnam|la plateform|m2i|simplon|adrar|doranco|cegos|le wagon'
    regex_desc_ecole = "(?i)école partenaire|ecole partenaire|centre de formation|spécialiste de la formation|dans le cadre d'une formation|pour le compte d'une école|école de commerce|cabinet de recrutement|titre rncp|organisme de formation|ecole de commerce"

    if 'entreprise.nom' in df_spark.columns:
        df_spark = df_spark.filter(~F.col('`entreprise.nom`').rlike(regex_ecoles))
    
    if 'description' in df_spark.columns:
        df_spark = df_spark.filter(~F.col('description').rlike(regex_desc_ecole))

    # 6. Filtrage 4 : Strictement Île-de-France et Lille / Nord uniquement
    regex_zones_autorisees = '(?i)75|77|78|91|92|93|94|95|île-de-france|ile-de-france|paris|59|lille|villeneuve|roubaix|tourcoing|seclin|nord'
    if 'lieuTravail.libelle' in df_spark.columns:
        df_spark = df_spark.filter(F.col('`lieuTravail.libelle`').rlike(regex_zones_autorisees))

    # 7. Filtrage 5 : Conserver UNIQUEMENT les contrats d'alternance de 24 mois (2 ans)
    regex_24m = '(?i)24\\s*mois|2\\s*ans|24\\s*months|2\\s*years'
    if 'typeContratLibelle' in df_spark.columns:
        df_spark = df_spark.filter(
            F.col('typeContratLibelle').rlike('(?i)24\\s*mois') | F.col('intitule').rlike(regex_24m) | F.col('description').rlike(regex_24m)
        )

    nb_filtre = df_spark.count()
    print(f"🎯 Nombre d'offres après tous les filtrages (Alternance + Entreprises + 24 mois uniquement) : {nb_filtre}")

    if nb_filtre == 0:
        print("⚠️ Aucune offre ne correspond à tes critères stricts pour le moment.")
        spark.stop()
        return

    # 7. EXTRACTION BIG DATA DE COMPÉTENCES TECH AVEC PYSPARK
    df_spark = df_spark.withColumn('text_full', F.lower(F.concat_ws(' ', F.col('intitule'), F.col('description'))))
    
    # Création du lien direct vers l'offre (LinkedIn vs France Travail)
    if 'url_offre' in df_spark.columns:
        df_spark = df_spark.withColumn(
            'url_offre',
            F.when((F.col('url_offre').isNotNull()) & (F.col('url_offre') != '') & (F.col('url_offre') != 'None') & (F.col('url_offre') != 'nan') & (~F.col('url_offre').rlike('(?i)francetravail')), F.col('url_offre'))
            .when(F.col('id').rlike('^\\d{9,}$'), F.concat(F.lit("https://www.linkedin.com/jobs/view/"), F.col('id')))
            .otherwise(F.concat(F.lit("https://candidat.francetravail.fr/offres/recherche/detail/"), F.col('id')))
        )
    else:
        df_spark = df_spark.withColumn(
            'url_offre',
            F.when(F.col('id').rlike('^\\d{9,}$'), F.concat(F.lit("https://www.linkedin.com/jobs/view/"), F.col('id')))
            .otherwise(F.concat(F.lit("https://candidat.francetravail.fr/offres/recherche/detail/"), F.col('id')))
        )

    skill_cols = [F.when(F.col('text_full').rlike(rf'\b{s}\b'), F.lit(s)).otherwise(None) for s in LISTE_SKILLS_TECH]
    df_spark = df_spark.withColumn('competences_array', F.array(*skill_cols))
    df_spark = df_spark.withColumn('competences_tech', F.concat_ws(', ', F.array_compact('competences_array')))

    # 8. Sélection et renommage des colonnes finales
    colonnes_a_garder = {
        'id': 'id_offre',
        'intitule': 'titre_poste',
        'entreprise.nom': 'nom_entreprise',
        'lieuTravail.libelle': 'localisation',
        'typeContrat': 'type_contrat',
        'typeContratLibelle': 'duree_contrat',
        'natureContrat': 'nature_contrat',
        'salaire.libelle': 'salaire',
        'competences_tech': 'competences_tech',
        'dateCreation': 'date_publication',
        'url_offre': 'url_offre'
    }

    cols_existantes = [c for c in colonnes_a_garder.keys() if c in df_spark.columns]
    
    # Sélection des colonnes avec backticks pour PySpark
    df_final_spark = df_spark.select([F.col(f'`{c}`') for c in cols_existantes])

    for old_col, new_col in colonnes_a_garder.items():
        if old_col in cols_existantes:
            df_final_spark = df_final_spark.withColumnRenamed(old_col, new_col)

    # Conversion en Pandas pour export CSV propre
    df_silver = df_final_spark.toPandas()
    df_silver.fillna('Non renseigné', inplace=True)

    spark.stop()

    # 9. Sauvegarde dans data/silver/
    os.makedirs('data/silver', exist_ok=True)
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_filename = f"data/silver/offres_cibles_idf_lille_{today}.csv"
    
    df_silver.to_csv(silver_filename, index=False, encoding='utf-8')
    print(f"💎 Données PySpark nettoyées et sauvegardées dans : {silver_filename}")

if __name__ == "__main__":
    process_silver_layer()