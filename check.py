# Dans check.py — stats finales avant commit
import duckdb

conn = duckdb.connect("data/openmarches.duckdb")

print("=== REPARTITION PAR TYPE CODE GEO ===")
print(conn.execute("""
    SELECT lieuExecution.typeCode as type_code,
        COUNT(*) as nb,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
    FROM raw_marches_full
    WHERE lieuExecution IS NOT NULL
    AND codeCPV LIKE '72%'
    GROUP BY type_code
    ORDER BY nb DESC
    LIMIT 8
""").fetchdf().to_string(index=False))

conn.close()