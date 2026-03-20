-- Staging : marchés publics nettoyés
-- Source : raw_marches (chargée depuis DECP JSON)
-- Filtre les montants aberrants et normalise les types

SELECT
    id                                          AS marche_id,
    CAST(montant AS DOUBLE)                     AS montant_eur,
    CAST(dureeMois AS INTEGER)                  AS duree_mois,
    objet                                       AS objet,
    codeCPV                                     AS code_cpv,
    LEFT(codeCPV, 2)                            AS famille_cpv,
    procedure                                   AS procedure_type,
    dateNotification                            AS date_notification,
    CAST(EXTRACT(YEAR FROM CAST(dateNotification AS DATE)) AS VARCHAR)                   AS annee,
    acheteur['id']                               AS acheteur_siret,
    titulaires[1]['titulaire']['id']            AS titulaire_siret_1,

    -- Flags qualite
    CASE WHEN montant <= 1 THEN TRUE ELSE FALSE END           AS flag_montant_suspect,
    CASE WHEN montant > 1e10 THEN TRUE ELSE FALSE END         AS flag_montant_aberrant,
    CASE WHEN CAST(EXTRACT(YEAR FROM CAST(dateNotification AS DATE)) AS VARCHAR) < '2018' THEN TRUE
         WHEN CAST(EXTRACT(YEAR FROM CAST(dateNotification AS DATE)) AS VARCHAR) > '2026' THEN TRUE
         ELSE FALSE END                                        AS flag_date_suspecte

FROM raw_marches_full
WHERE montant IS NOT NULL
  AND montant < 500000000   -- exclure les montants manifestement aberrants
  AND dateNotification IS NOT NULL
