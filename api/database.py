import duckdb, math
from pathlib import Path

# Railway met les fichiers dans /app par defaut
# En local c'est le dossier du projet
DB_PATH = Path(__file__).parent.parent / "data" / "openmarches_prod.duckdb"

def get_conn():
    return duckdb.connect(str(DB_PATH), read_only=True)

def clean(val):
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def query(sql: str, params: list = None) -> list[dict]:
    conn = get_conn()
    try:
        if params:
            result = conn.execute(sql, params).fetchdf()
        else:
            result = conn.execute(sql).fetchdf()
        records = result.to_dict(orient="records")
        return [{k: clean(v) for k, v in row.items()} for row in records]
    finally:
        conn.close()