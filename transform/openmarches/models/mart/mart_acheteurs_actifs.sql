-- Mart : Acheteurs publics — volume et régularité de publication

WITH base AS (
    SELECT * FROM {{ ref('stg_titulaires') }}
    WHERE flag_montant_suspect = FALSE
      AND flag_date_suspecte = FALSE
      AND annee BETWEEN '2018' AND '2026'
)

SELECT
    acheteur_siret,
    COUNT(*)                                AS nb_marches_total,
    COUNT(DISTINCT annee)                   AS nb_annees_actives,
    COUNT(DISTINCT CASE WHEN famille_cpv = '72' THEN marche_id END)
                                            AS nb_marches_it,
    ROUND(SUM(montant_eur) / 1e6, 2)       AS total_millions_eur,
    ROUND(SUM(CASE WHEN famille_cpv = '72'
        THEN montant_eur ELSE 0 END) / 1e6, 2)
                                            AS total_millions_it,
    MIN(annee)                              AS premiere_annee,
    MAX(annee)                              AS derniere_annee,
    CASE
        WHEN COUNT(DISTINCT annee) >= 5 THEN 'REGULIER'
        WHEN COUNT(DISTINCT annee) >= 2 THEN 'OCCASIONNEL'
        ELSE 'PONCTUEL'
    END                                     AS profil_publication

FROM base
GROUP BY acheteur_siret
HAVING COUNT(*) >= 3
ORDER BY total_millions_eur DESC
