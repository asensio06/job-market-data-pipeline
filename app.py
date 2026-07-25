import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Job Market Data Pipeline - Alternances 24 Mois",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS pour un design moderne & sobre
st.markdown("""
    <style>
    .main {
        background-color: #0f172a;
        color: #f8fafc;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid #475569;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .metric-val {
        font-size: 2.2rem;
        font-weight: 700;
        color: #38bdf8;
    }
    .metric-lbl {
        font-size: 0.95rem;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    </style>
""", unsafe_allow_html=True)

DB_PATH = "data/job_market.db"

@st.cache_data(ttl=60)
def load_data():
    """Charge les données depuis DuckDB (Tables et Vues dbt/Gold)."""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Table des faits dbt / silver_offres
    df_offres = con.execute("""
        SELECT 
            id_offre, 
            titre_poste, 
            nom_entreprise, 
            localisation, 
            zone_geographique, 
            type_contrat, 
            duree_contrat, 
            salaire, 
            competences_tech, 
            date_publication 
        FROM silver_offres
        ORDER BY date_publication DESC
    """).fetchdf()

    # Vue Gold Compétences
    df_skills = con.execute("""
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
        ORDER BY nb_demandes DESC
    """).fetchdf()

    # Vue Gold Zones
    df_zones = con.execute("""
        SELECT zone_geographique, COUNT(*) as count 
        FROM silver_offres 
        GROUP BY zone_geographique
    """).fetchdf()

    con.close()
    return df_offres, df_skills, df_zones

# Chargement des données
try:
    df_offres, df_skills, df_zones = load_data()
except Exception as e:
    st.error(f"❌ Erreur lors de la connexion à la base DuckDB (`{DB_PATH}`) : {e}")
    st.stop()

# Header Principal
st.title("⚡ Job Market Data Pipeline — Alternances 24 Mois")
st.caption("Dashboard analytique en direct • Métiers Data • Île-de-France & Lille / Nord • Source : France Travail API (PySpark & dbt)")

st.markdown("---")

# KPIs Principaux
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-val">{len(df_offres)}</div>
            <div class="metric-lbl">Offres Ciblées (24 Mois)</div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    nb_entreprises = df_offres['nom_entreprise'].nunique()
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-val">{nb_entreprises}</div>
            <div class="metric-lbl">Entreprises Recruteuses</div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    top_skill = df_skills.iloc[0]['competence_tech'].upper() if not df_skills.empty else "N/A"
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-val">{top_skill}</div>
            <div class="metric-lbl">Skill #1 Demandé</div>
        </div>
    """, unsafe_allow_html=True)

with col4:
    nb_zones = df_offres['zone_geographique'].nunique()
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-val">{nb_zones} Zones</div>
            <div class="metric-lbl">Périmètre (IDF + Lille)</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Barre Latérale de Filtres
st.sidebar.header("🔍 Filtres de recherche")

zone_filter = st.sidebar.multiselect(
    "Zone géographique",
    options=df_offres['zone_geographique'].unique(),
    default=df_offres['zone_geographique'].unique()
)

entreprises_filter = st.sidebar.multiselect(
    "Entreprise",
    options=df_offres['nom_entreprise'].unique(),
    default=[]
)

search_keyword = st.sidebar.text_input("Mot-clé (titre ou skill)", "")

# Application des filtres
df_filtered = df_offres[df_offres['zone_geographique'].isin(zone_filter)]

if entreprises_filter:
    df_filtered = df_filtered[df_filtered['nom_entreprise'].isin(entreprises_filter)]

if search_keyword:
    kw = search_keyword.lower()
    df_filtered = df_filtered[
        df_filtered['titre_poste'].str.lower().str.contains(kw) |
        df_filtered['competences_tech'].str.lower().str.contains(kw) |
        df_filtered['nom_entreprise'].str.lower().str.contains(kw)
    ]

# Section Graphiques
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("💻 Top Compétences Tech recherchées (PySpark NLP)")
    if not df_skills.empty:
        fig_skills = px.bar(
            df_skills.head(10),
            x='nb_demandes',
            y='competence_tech',
            orientation='h',
            labels={'nb_demandes': "Nombre d'offres", 'competence_tech': "Compétence"},
            color='nb_demandes',
            color_continuous_scale='Blues'
        )
        fig_skills.update_layout(yaxis={'categoryorder': 'total ascending'}, template="plotly_dark", height=380)
        st.plotly_chart(fig_skills, use_container_width=True)
    else:
        st.info("Aucune compétence extraite.")

with col_right:
    st.subheader("📍 Répartition par Zone Géographique")
    if not df_zones.empty:
        fig_zones = px.pie(
            df_zones,
            names='zone_geographique',
            values='count',
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Teal
        )
        fig_zones.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_zones, use_container_width=True)

# Explorateur d'offres
st.markdown("---")
st.subheader("📋 Liste des Offres d'Alternance Data (24 Mois)")

st.dataframe(
    df_filtered[[
        'id_offre', 'titre_poste', 'nom_entreprise', 
        'localisation', 'duree_contrat', 'competences_tech', 'date_publication'
    ]],
    column_config={
        "id_offre": "ID Offre",
        "titre_poste": "Titre du poste",
        "nom_entreprise": "Entreprise",
        "localisation": "Localisation",
        "duree_contrat": "Durée Contrat",
        "competences_tech": "Compétences Tech (Spark NLP)",
        "date_publication": "Date Publication"
    },
    use_container_width=True,
    hide_index=True
)

st.caption("Projet Data Engineering End-to-End • Python / PySpark / DuckDB / dbt / Airflow / Streamlit")
