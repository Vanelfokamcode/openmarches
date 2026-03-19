import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "openmarches.duckdb"

def get_conn():
    """Retourne une connexion DuckDB en lecture seule."""
    return duckdb.connect(str(DB_PATH), read_only=True)

def query(sql: str, params: list = None) -> list[dict]:
    """Execute une requete et retourne une liste de dicts."""
    conn = get_conn()
    try:
        if params:
            result = conn.execute(sql, params).fetchdf()
        else:
            result = conn.execute(sql).fetchdf()
        return result.to_dict(orient="records")
    finally:
        conn.close()
