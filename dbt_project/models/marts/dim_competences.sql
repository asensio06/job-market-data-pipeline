WITH unnested_skills AS (
    SELECT DISTINCT
        trim(skill) AS nom_competence
    FROM (
        SELECT unnest(string_split(competences_tech, ',')) AS skill
        FROM {{ ref('stg_offres') }}
        WHERE competences_tech IS NOT NULL 
          AND competences_tech != 'Non renseigné' 
          AND competences_tech != ''
    )
)

SELECT 
    md5(nom_competence) AS id_competence,
    nom_competence
FROM unnested_skills
