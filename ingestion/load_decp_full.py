import duckdb, json, pandas as pd
from pathlib import Path
import time

DB_PATH = "data/openmarches.duckdb"
JSON_PATH = "data/raw/decp_full.json"

print(f"Lecture de {Path(JSON_PATH).stat().st_size / 1e6:.0f} Mo...")
t0 = time.time()

with open(JSON_PATH) as f:
    data = json.load(f)

marches = data["marches"]["marche"]
print(f"Marchés trouvés : {len(marches)} — lecture en {time.time()-t0:.0f}s")

print("Normalisation pandas...")
t1 = time.time()
df = pd.json_normalize(marches)
print(f"Colonnes : {len(df.columns)} — normalisation en {time.time()-t1:.0f}s")

print("Chargement dans DuckDB...")
t2 = time.time()
conn = duckdb.connect(DB_PATH)
conn.execute("CREATE OR REPLACE TABLE raw_marches_full AS SELECT * FROM df")
count = conn.execute("SELECT COUNT(*) FROM raw_marches_full").fetchone()[0]
print(f"Marchés dans DuckDB : {count} — chargement en {time.time()-t2:.0f}s")
print(f"Temps total : {time.time()-t0:.0f}s")
conn.close()
