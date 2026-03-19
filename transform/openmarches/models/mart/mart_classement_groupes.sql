-- Mart : Classement des groupes ESN par volume marchés IT publics
-- Source : stg_titulaires
-- Granularite : 1 ligne par groupe + annee

WITH base AS (
    SELECT * FROM {{ ref('stg_titulaires') }}
    WHERE famille_cpv = '72'      -- marchés IT uniquement
      AND annee BETWEEN '2018' AND '2026'
      AND flag_montant_suspect = FALSE
),

par_groupe_annee AS (
    SELECT
        groupe,
        annee,
        COUNT(*)                            AS nb_marches,
        COUNT(DISTINCT siret)               AS nb_entites_juridiques,
        COUNT(DISTINCT acheteur_siret)      AS nb_acheteurs_distincts,
        ROUND(SUM(montant_eur) / 1e6, 2)    AS total_millions_eur,
        ROUND(AVG(montant_eur) / 1e3, 1)    AS moy_milliers_eur,
        ROUND(MAX(montant_eur) / 1e6, 2)    AS max_marche_millions_eur
    FROM base
    GROUP BY groupe, annee
),

total_par_annee AS (
    SELECT annee, SUM(total_millions_eur) AS total_annee_M
    FROM par_groupe_annee
    GROUP BY annee
)

SELECT
    p.groupe,
    p.annee,
    p.nb_marches,
    p.nb_entites_juridiques,
    p.nb_acheteurs_distincts,
    p.total_millions_eur,
    p.moy_milliers_eur,
    p.max_marche_millions_eur,
    ROUND(p.total_millions_eur / t.total_annee_M * 100, 1) AS part_marche_pct
FROM par_groupe_annee p
JOIN total_par_annee t ON p.annee = t.annee
ORDER BY p.annee DESC, p.total_millions_eur DESC
