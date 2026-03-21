"""
Script de startup pour Railway.
Telecharge le fichier DECP reduit et charge dans DuckDB
si la base n'existe pas encore.
"""
import duckdb, json, requests, os
from pathlib import Path

DB_PATH = Path("data/openmarches.duckdb")
JSON_PATH = Path("data/raw/decp_sample.json")
DECP_URL = "https://www.data.gouv.fr/api/1/datasets/r/2551ad40-584a-42fd-b3cc-e8906183287e"

def init_db():
    if DB_PATH.exists():
        conn = duckdb.connect(str(DB_PATH))
        try:
            n = conn.execute("SELECT COUNT(*) FROM raw_marches").fetchone()[0]
            if n > 0:
                print(f"DB existante : {n} marches")
                conn.close()
                return
        except:
            pass
        conn.close()

    print("Telechargement DECP...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    r = requests.get(DECP_URL, stream=True, timeout=60)
    with open(JSON_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Fichier : {JSON_PATH.stat().st_size / 1e6:.0f} Mo")

    import pandas as pd
    with open(JSON_PATH) as f:
        data = json.load(f)
    marches = data["marches"]["marche"]
    df = pd.json_normalize(marches)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE OR REPLACE TABLE raw_marches AS SELECT * FROM df")
    print(f"Marches charges : {conn.execute('SELECT COUNT(*) FROM raw_marches').fetchone()[0]}")

    # Charger le referentiel
    import csv
    ref_path = Path("transform/seeds/ref_groupes_esn.csv")
    if ref_path.exists():
        import pandas as pd
        ref = pd.read_csv(str(ref_path), dtype=str)
        conn.execute("CREATE OR REPLACE TABLE ref_groupes_esn AS SELECT * FROM ref")

    conn.close()
    print("DB initialisee")

if __name__ == "__main__":
    init_db()