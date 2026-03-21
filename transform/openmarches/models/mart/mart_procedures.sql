-- Mart : Analyse des procedures d'attribution
-- Identifie les acheteurs qui evitent la mise en concurrence

WITH base AS (
    SELECT
        acheteur_siret,
        procedure_type,
        montant_eur,
        annee,
        CASE
            WHEN procedure_type = 'Appel d''offres ouvert'
                THEN 'OUVERT'
            WHEN procedure_type = 'Appel d''offres restreint'
                THEN 'RESTREINT'
            WHEN procedure_type = 'Marché passé sans publicité ni mise en concurrence préalable'
                THEN 'SANS_CONCURRENCE'
            WHEN procedure_type = 'Procédure adaptée'
                THEN 'PROCEDURE_ADAPTEE'
            WHEN procedure_type = 'Procédure avec négociation'
                THEN 'AVEC_NEGOCIATION'
            ELSE 'AUTRE'
        END AS categorie_procedure
    FROM {{ ref('stg_titulaires') }}
    WHERE flag_montant_suspect = FALSE
    AND annee BETWEEN '2018' AND '2026'
    AND procedure_type IS NOT NULL
)

SELECT
    acheteur_siret,
    categorie_procedure,
    COUNT(*) AS nb_marches,
    ROUND(SUM(montant_eur)/1e6, 2) AS total_millions_eur,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY acheteur_siret), 1)
        AS pct_de_lacheteur
FROM base
GROUP BY acheteur_siret, categorie_procedure
HAVING COUNT(*) >= 3
ORDER BY acheteur_siret, nb_marches DESC