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

    # 1. Identifier le fichier JSON le plus récent dans Bronze
    bronze_files = glob.glob('data/bronze/*.json')
    if not bronze_files:
        print("❌ Aucun fichier trouvé dans data/bronze/.")
        return
    
    latest_file = max(bronze_files, key=os.path.getctime)
    print(f"📄 Lecture du fichier brut : {latest_file}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    offres = data.get('resultats', data) if isinstance(data, dict) else data
    if not offres:
        print("⚠️ Le fichier JSON est vide.")
        return

    # Pandas normalization pour garantir la propreté de la structure
    df_pd = pd.json_normalize(offres).astype(str)
    print(f"📊 Nombre total d'offres avant filtrage : {len(df_pd)}")

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

    # 4. Filtrage 2 : Intitulés de postes ciblés
    mots_cles_postes = '(?i)data analyst|data scientist|data engineer|qualité et de la performance|business analyst|data manager|data'
    if 'intitule' in df_spark.columns:
        df_spark = df_spark.filter(F.col('intitule').rlike(mots_cles_postes))

    # 5. Filtrage 3 : Exclure les écoles / organismes de formation
    regex_ecoles = '(?i)iscod|kaischool|openclassrooms|studi|pigier|epitech|cfa|ecole|école|campus|euridis|mbway|formation|imc'
    regex_desc_ecole = "(?i)école partenaire|ecole partenaire|centre de formation|spécialiste de la formation|dans le cadre d'une formation|pour le compte d'une école|école de commerce|cabinet de recrutement"

    if 'entreprise.nom' in df_spark.columns:
        df_spark = df_spark.filter(~F.col('`entreprise.nom`').rlike(regex_ecoles))
    
    if 'description' in df_spark.columns:
        df_spark = df_spark.filter(~F.col('description').rlike(regex_desc_ecole))

    # 6. Filtrage 4 : Conserver UNIQUEMENT les contrats de 24 mois (CDD - 24 Mois)
    if 'typeContratLibelle' in df_spark.columns:
        df_spark = df_spark.filter(F.col('typeContratLibelle').rlike('(?i)24\\s*mois'))

    nb_filtre = df_spark.count()
    print(f"🎯 Nombre d'offres après tous les filtrages (Alternance + Entreprises + 24 mois uniquement) : {nb_filtre}")

    if nb_filtre == 0:
        print("⚠️ Aucune offre ne correspond à tes critères stricts pour le moment.")
        spark.stop()
        return

    # 7. EXTRACTION BIG DATA DE COMPÉTENCES TECH AVEC PYSPARK
    df_spark = df_spark.withColumn('text_full', F.lower(F.concat_ws(' ', F.col('intitule'), F.col('description'))))
    
    # Création du lien direct vers l'offre sur France Travail
    df_spark = df_spark.withColumn(
        'url_offre', 
        F.concat(F.lit("https://candidat.francetravail.fr/offres/recherche/detail/"), F.col('id'))
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