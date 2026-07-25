WITH raw_entreprises AS (
    SELECT DISTINCT
        md5(nom_entreprise) AS id_entreprise,
        nom_entreprise
    FROM {{ ref('stg_offres') }}
)

SELECT * FROM raw_entreprises
