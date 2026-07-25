WITH raw_locations AS (
    SELECT DISTINCT
        md5(localisation) AS id_localisation,
        localisation,
        zone_geographique
    FROM {{ ref('stg_offres') }}
)

SELECT * FROM raw_locations
