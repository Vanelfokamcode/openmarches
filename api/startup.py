import duckdb, requests, os
from pathlib import Path

DB_PATH = Path("data/openmarches.duckdb")
JSON_PATH = Path("data/raw/decp_sample.json")
DECP_URL = "https://www.data.gouv.fr/api/1/datasets/r/2551ad40-584a-42fd-b3cc-e8906183287e"

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not JSON_PATH.exists():
        print("Telechargement DECP...")
        r = requests.get(DECP_URL, stream=True, timeout=120)
        with open(JSON_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Fichier : {JSON_PATH.stat().st_size / 1e6:.0f} Mo")

    if not DB_PATH.exists():
        print("Chargement DuckDB direct...")
        conn = duckdb.connect(str(DB_PATH))
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_marches AS
            SELECT * FROM read_json('{JSON_PATH}',
                format='array',
                maximum_object_size=500000000,
                ignore_errors=true)
        """)
        n = conn.execute("SELECT COUNT(*) FROM raw_marches").fetchone()[0]
        print(f"Marches : {n}")
        conn.close()
    print("DB prete")

if __name__ == "__main__":
    init_db()