WITH source_data AS (
    SELECT 
        id_offre,
        trim(titre_poste) AS titre_poste,
        coalesce(trim(nom_entreprise), 'Non renseigné') AS nom_entreprise,
        trim(localisation) AS localisation,
        coalesce(trim(zone_geographique), 'Île-de-France') AS zone_geographique,
        type_contrat,
        duree_contrat,
        nature_contrat,
        salaire,
        competences_tech,
        TRY_CAST(date_publication AS TIMESTAMP) AS date_publication,
        date_insertion
    FROM {{ source('duckdb_source', 'silver_offres') }}
)

SELECT * FROM source_data
