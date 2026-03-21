import duckdb
conn = duckdb.connect("data/openmarches_prod.duckdb")
print(conn.execute("SHOW TABLES").fetchdf().to_string())
conn.close()