-- Mart : Relations fideles acheteur-fournisseur
-- Identifie les couples qui se retrouvent 5+ fois sur des marches IT

WITH base AS (
    SELECT * FROM {{ ref('stg_titulaires') }}
    WHERE flag_montant_suspect = FALSE
    AND annee BETWEEN '2018' AND '2026'
    AND famille_cpv = '72'
)

SELECT
    acheteur_siret,
    siret AS titulaire_siret,
    groupe,
    nom_entreprise,
    COUNT(*) AS nb_marches,
    COUNT(DISTINCT annee) AS nb_annees_communes,
    ROUND(SUM(montant_eur)/1e6, 2) AS total_millions_eur,
    MIN(date_notification) AS premier_marche,
    MAX(date_notification) AS dernier_marche,
    ROUND(AVG(montant_eur)/1e3, 1) AS montant_moyen_milliers,
    CASE
        WHEN COUNT(*) >= 10 THEN 'TRES_FREQUENT'
        WHEN COUNT(*) >= 7  THEN 'FREQUENT'
        ELSE 'REGULIER'
    END AS niveau_recurrence
FROM base
WHERE siret IS NOT NULL
AND acheteur_siret IS NOT NULL
GROUP BY acheteur_siret, siret, groupe, nom_entreprise
HAVING COUNT(*) >= 5
ORDER BY nb_marches DESC