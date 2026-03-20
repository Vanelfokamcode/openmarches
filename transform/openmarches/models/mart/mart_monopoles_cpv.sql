-- Mart : Parts de marché par code CPV précis
-- Identifie les monopoles sectoriels sur les marchés IT publics
-- Source : stg_titulaires

WITH base AS (
    SELECT * FROM {{ ref('stg_titulaires') }}
    WHERE flag_montant_suspect = FALSE
    AND annee BETWEEN '2018' AND '2026'
    AND montant_eur < 500000000
),

total_par_cpv AS (
    SELECT code_cpv,
        COUNT(*) as nb_total,
        SUM(montant_eur) as montant_total
    FROM base
    GROUP BY code_cpv
    HAVING COUNT(*) >= 5
),

par_groupe_cpv AS (
    SELECT
        b.code_cpv,
        b.famille_cpv,
        b.groupe,
        COUNT(*) as nb_marches,
        ROUND(SUM(b.montant_eur)/1e6, 2) as total_millions_eur,
        ROUND(SUM(b.montant_eur) * 100.0 / t.montant_total, 1) as part_pct
    FROM base b
    JOIN total_par_cpv t ON b.code_cpv = t.code_cpv
    WHERE b.groupe NOT IN ('NON MAPPE', 'INDEPENDANT')
    GROUP BY b.code_cpv, b.famille_cpv, b.groupe, t.montant_total
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY code_cpv ORDER BY total_millions_eur DESC) as rang
    FROM par_groupe_cpv
)

SELECT
    code_cpv,
    famille_cpv,
    groupe,
    rang,
    nb_marches,
    total_millions_eur,
    part_pct,
    CASE WHEN part_pct >= 70 THEN 'MONOPOLE'
         WHEN part_pct >= 50 THEN 'DOMINANT'
         WHEN part_pct >= 30 THEN 'MAJEUR'
         ELSE 'PRESENT'
    END as niveau_concentration
FROM ranked
WHERE rang <= 3
ORDER BY total_millions_eur DESC