-- Staging : titulaires avec groupe économique identifié
-- Joint stg_marches avec le référentiel des groupes ESN

WITH marches AS (
    SELECT * FROM {{ ref('stg_marches') }}
),
groupes AS (
    SELECT * FROM {{ ref('ref_groupes_esn') }}
)

SELECT
    m.marche_id,
    m.titulaire_siret_1                         AS siret,
    g.nom_entreprise,
    COALESCE(g.groupe, 'NON MAPPE')             AS groupe,
    m.montant_eur,
    m.duree_mois,
    m.objet,
    m.code_cpv,
    m.famille_cpv,
    m.procedure_type,
    m.date_notification,
    m.annee,
    m.acheteur_siret,
    m.flag_montant_suspect,
    m.flag_date_suspecte,

    -- Classification CPV
    CASE
        WHEN m.famille_cpv = '72' THEN 'IT_SERVICES'
        WHEN m.famille_cpv = '48' THEN 'SOFTWARE'
        WHEN m.famille_cpv = '32' THEN 'TELECOM_EQUIPMENT'
        WHEN m.famille_cpv = '30' THEN 'COMPUTERS'
        ELSE 'AUTRES'
    END AS categorie_it

FROM marches m
LEFT JOIN groupes g ON m.titulaire_siret_1 = g.siret
WHERE m.flag_montant_aberrant = FALSE
