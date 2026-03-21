import duckdb

conn = duckdb.connect("data/openmarches.duckdb")

print("=== MART RECURRENTS TOP 5 ===")
print(conn.execute("SELECT * FROM main.mart_recurrents LIMIT 5").fetchdf().to_string(index=False))

print("\n=== PROCEDURES STATS ===")
print(conn.execute("""
    SELECT categorie_procedure,
        SUM(nb_marches) as nb_total,
        ROUND(SUM(total_millions_eur),1) as total_M
    FROM main.mart_procedures
    GROUP BY categorie_procedure
    ORDER BY nb_total DESC
""").fetchdf().to_string(index=False))

conn.close()