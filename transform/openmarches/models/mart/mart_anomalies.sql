-- Mart : Anomalies détectées dans les données DECP
-- Documente les cas suspects pour la page "limites et transparence"

WITH marches AS (
    SELECT * FROM {{ ref('stg_marches') }}
),

-- Marchés à 1 euro ou moins
montants_suspects AS (
    SELECT
        'MONTANT_SUSPECT'               AS type_anomalie,
        marche_id,
        acheteur_siret,
        montant_eur,
        date_notification,
        annee,
        objet,
        'Montant <= 1 euro publié'      AS description
    FROM marches
    WHERE flag_montant_suspect = TRUE
),

-- Dates impossibles
dates_suspectes AS (
    SELECT
        'DATE_SUSPECTE'                 AS type_anomalie,
        marche_id,
        acheteur_siret,
        montant_eur,
        date_notification,
        annee,
        objet,
        'Date de notification hors 2018-2026' AS description
    FROM marches
    WHERE flag_date_suspecte = TRUE
      AND date_notification IS NOT NULL
)

SELECT * FROM montants_suspects
UNION ALL
SELECT * FROM dates_suspectes
ORDER BY type_anomalie, annee
