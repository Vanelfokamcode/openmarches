-- Mart geo simplifie -- sans seed population pour eviter les erreurs de type
-- On agrege directement par departement extrait du lieuExecution STRUCT

SELECT
    CASE
        WHEN lieuExecution.typeCode = 'Code département'
            THEN lieuExecution.code
        WHEN lieuExecution.typeCode IN ('Code postal','Code commune','Code arrondissement')
            THEN LEFT(lieuExecution.code, 2)
        ELSE NULL
    END AS dept,
    COUNT(CASE WHEN LEFT(codeCPV,2) = '72' THEN 1 END) as nb_marches_it,
    ROUND(SUM(CASE WHEN LEFT(codeCPV,2) = '72' THEN montant ELSE 0 END)/1e6, 2) as total_M_it,
    ROUND(SUM(CASE WHEN LEFT(codeCPV,2) = '48' THEN montant ELSE 0 END)/1e6, 2) as total_M_logiciels,
    ROUND(SUM(CASE WHEN LEFT(codeCPV,2) = '64' THEN montant ELSE 0 END)/1e6, 2) as total_M_telecom,
    ROUND(SUM(CASE WHEN LEFT(codeCPV,2) = '30' THEN montant ELSE 0 END)/1e6, 2) as total_M_materiel,
    ROUND(SUM(montant)/1e6, 2) as total_M_tous
FROM {{ source('openmarches', 'raw_marches_full') }}
WHERE montant IS NOT NULL
AND montant < 500000000
AND lieuExecution IS NOT NULL
AND lieuExecution.typeCode IS NOT NULL
GROUP BY dept
HAVING dept IS NOT NULL
AND LENGTH(dept) = 2
AND dept NOT IN ('00','99','CO')
ORDER BY total_M_it DESC