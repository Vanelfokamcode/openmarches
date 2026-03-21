"""
Script de mise a jour mensuelle OpenMarches.
Telecharge le nouveau DECP, recharge DuckDB, rebuild prod, push GitHub.
"""
import subprocess, requests, json, duckdb, pandas as pd
from pathlib import Path
from datetime import datetime

DECP_URL = "https://www.data.gouv.fr/api/1/datasets/r/2551ad40-584a-42fd-b3cc-e8906183287e"
JSON_PATH = Path("data/raw/decp_sample.json")
DB_PATH = Path("data/openmarches.duckdb")
PROD_DB_PATH = Path("data/openmarches_prod.duckdb")

def check_new_version():
    """Verifie si un nouveau fichier est disponible."""
    r = requests.head(DECP_URL, timeout=10)
    last_modified = r.headers.get("Last-Modified", "")
    print("Last-Modified DECP : " + last_modified)
    return last_modified

def download_decp():
    """Telecharge le fichier DECP."""
    print("Telechargement DECP...")
    r = requests.get(DECP_URL, stream=True, timeout=300)
    with open(JSON_PATH, "wb") as f:
        total = 0
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
            total += len(chunk)
    print("Taille : " + str(round(total / 1e6, 0)) + " Mo")

def reload_duckdb():
    """Recharge raw_marches depuis le JSON."""
    print("Rechargement DuckDB...")
    with open(JSON_PATH) as f:
        data = json.load(f)
    marches = data["marches"]["marche"]
    df = pd.json_normalize(marches)
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE OR REPLACE TABLE raw_marches AS SELECT * FROM df")
    n = conn.execute("SELECT COUNT(*) FROM raw_marches").fetchone()[0]
    print("Marches : " + str(n))
    conn.close()

def run_dbt():
    """Relance dbt seed + run."""
    print("dbt seed + run...")
    result = subprocess.run(
        ["dbt", "seed"],
        cwd="transform/openmarches",
        capture_output=True, text=True
    )
    print(result.stdout[-500:] if result.stdout else "")
    result = subprocess.run(
        ["dbt", "run"],
        cwd="transform/openmarches",
        capture_output=True, text=True
    )
    print(result.stdout[-500:] if result.stdout else "")

def rebuild_prod():
    """Rebuild le fichier prod minimaliste."""
    print("Rebuild prod DB...")
    src = duckdb.connect(str(DB_PATH))
    if PROD_DB_PATH.exists():
        PROD_DB_PATH.unlink()
    prod = duckdb.connect(str(PROD_DB_PATH))

    tables = ["mart_classement_groupes","mart_anomalies","mart_acheteurs_actifs",
              "mart_depassements_reels","mart_monopoles_cpv","ref_groupes_esn"]
    for table in tables:
        try:
            df = src.execute("SELECT * FROM main." + table).fetchdf()
            prod.execute("CREATE TABLE " + table + " AS SELECT * FROM df")
            print("  " + table + " : " + str(len(df)) + " lignes")
        except Exception as e:
            print("  " + table + " ERREUR : " + str(e))

    df_stg = src.execute("""
        SELECT marche_id, groupe, siret, nom_entreprise,
            montant_eur, annee, code_cpv, famille_cpv,
            LEFT(objet, 150) as objet, acheteur_siret,
            flag_montant_suspect, flag_date_suspecte
        FROM main.stg_titulaires
        WHERE flag_montant_suspect = FALSE LIMIT 50000
    """).fetchdf()
    prod.execute("CREATE TABLE stg_titulaires AS SELECT * FROM df_stg")
    print("  stg_titulaires : " + str(len(df_stg)) + " lignes")

    src.close()
    prod.close()
    size = round(PROD_DB_PATH.stat().st_size / 1e6, 1)
    print("Prod DB : " + str(size) + " Mo")

def git_push():
    """Git add + commit + push."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    subprocess.run(["git", "add", "-f", "data/openmarches_prod.duckdb"])
    subprocess.run(["git", "commit", "-m",
        "chore: mise a jour mensuelle DECP " + date_str])
    subprocess.run(["git", "push", "origin", "main"])
    print("Git push OK")

def run_all():
    print("=== REFRESH OPENMARCHES " + datetime.now().strftime("%Y-%m-%d %H:%M") + " ===")
    download_decp()
    reload_duckdb()
    run_dbt()
    rebuild_prod()
    git_push()
    print("=== REFRESH TERMINE ===")

if __name__ == "__main__":
    run_all()