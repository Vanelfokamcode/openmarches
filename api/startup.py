import duckdb, math
from pathlib import Path

# En prod on utilise le fichier pre-genere
DB_PATH = Path(__file__).parent.parent / "data" / "openmarches_prod.duckdb"

def init_db():
    if DB_PATH.exists():
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        n = conn.execute("SELECT COUNT(*) FROM mart_classement_groupes").fetchone()[0]
        print("DB prod prete : " + str(n) + " lignes classement")
        conn.close()
    else:
        print("ERREUR : openmarches_prod.duckdb introuvable")

if __name__ == "__main__":
    init_db()