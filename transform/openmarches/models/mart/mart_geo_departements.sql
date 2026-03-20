-- Mart : Dépenses IT publiques par département
-- Croise les marchés IT avec la population INSEE
-- Forensic Angle 3 : inégalités territoriales numériques

WITH marches_geo AS (
    SELECT
        CASE
            WHEN lieuExecution.typeCode = 'Code département'
                THEN lieuExecution.code
            WHEN lieuExecution.typeCode IN ('Code postal','Code commune','Code arrondissement')
                THEN LEFT(lieuExecution.code, 2)
            ELSE NULL
        END AS dept,
        montant,
        codeCPV,
        LEFT(codeCPV, 2) as famille_cpv,
        EXTRACT(YEAR FROM CAST(dateNotification AS DATE)) as annee
    FROM {{ source('openmarches', 'raw_marches_full') }}
    WHERE montant IS NOT NULL
    AND montant < 500000000
    AND dateNotification IS NOT NULL
),

par_dept AS (
    SELECT
        dept,
        COUNT(CASE WHEN famille_cpv = '72' THEN 1 END) as nb_marches_it,
        ROUND(SUM(CASE WHEN famille_cpv = '72' THEN montant ELSE 0 END)/1e6, 2) as total_M_it,
        ROUND(SUM(CASE WHEN famille_cpv = '48' THEN montant ELSE 0 END)/1e6, 2) as total_M_logiciels,
        ROUND(SUM(CASE WHEN famille_cpv = '64' THEN montant ELSE 0 END)/1e6, 2) as total_M_telecom,
        ROUND(SUM(CASE WHEN famille_cpv = '30' THEN montant ELSE 0 END)/1e6, 2) as total_M_materiel,
        ROUND(SUM(montant)/1e6, 2) as total_M_tous_marches
    FROM marches_geo
    WHERE dept IS NOT NULL
    AND LENGTH(dept) = 2
    AND dept NOT IN ('00','99')
    GROUP BY dept
)

SELECT
    p.dept,
    r.nom_dept,
    r.region,
    r.population_2023,
    p.nb_marches_it,
    p.total_M_it,
    p.total_M_logiciels,
    p.total_M_telecom,
    p.total_M_materiel,
    p.total_M_tous_marches,
    CASE WHEN r.population_2023 > 0
        THEN ROUND(p.total_M_it * 1e6 / r.population_2023, 2)
        ELSE NULL
    END as euros_it_par_habitant
FROM par_dept p
LEFT JOIN {{ ref('ref_population_dept') }} r ON p.dept = r.dept
WHERE p.total_M_it > 0
ORDER BY p.total_M_it DESC