SELECT 
    o.id_offre,
    md5(o.nom_entreprise) AS id_entreprise,
    md5(o.localisation) AS id_localisation,
    o.titre_poste,
    o.type_contrat,
    o.duree_contrat,
    o.nature_contrat,
    o.salaire,
    o.competences_tech,
    o.date_publication,
    o.date_insertion
FROM {{ ref('stg_offres') }} o
