import duckdb
import json

DB_PATH = "data/openmarches.duckdb"
JSON_PATH = "data/raw/decp_sample.json"

print("Lecture du JSON...")
with open(JSON_PATH) as f:
    data = json.load(f)

marches = data["marches"]["marche"]
print(f"Marchés trouvés : {len(marches)}")

conn = duckdb.connect(DB_PATH)

# Charger via pandas pour gérer les structures imbriquées
import pandas as pd
df = pd.json_normalize(marches)
print(f"Colonnes après normalisation : {len(df.columns)}")
print(df.columns.tolist())

conn.execute("CREATE OR REPLACE TABLE raw_marches AS SELECT * FROM df")
count = conn.execute("SELECT COUNT(*) FROM raw_marches").fetchone()[0]
print(f"\nMarchés dans DuckDB : {count}")

# Stats rapides
print("\n=== MONTANTS ===")
print(conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(montant) as total_euros,
        AVG(montant) as moyenne,
        MIN(montant) as min_montant,
        MAX(montant) as max_montant
    FROM raw_marches
    WHERE montant IS NOT NULL
""").fetchdf().to_string())

print("\n=== MONTANTS SUSPECTS (<=1) ===")
n = conn.execute("SELECT COUNT(*) FROM raw_marches WHERE montant <= 1").fetchone()[0]
print(f"Marchés à 1€ ou moins : {n}")

conn.close()
