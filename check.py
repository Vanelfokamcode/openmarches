# Dans check.py en local — créer un DuckDB de prod minimaliste
import duckdb

src = duckdb.connect("data/openmarches.duckdb")
prod = duckdb.connect("data/openmarches_prod.duckdb")

# Copier seulement les marts — pas les raw tables
for table in ["mart_classement_groupes", "mart_anomalies",
              "mart_acheteurs_actifs", "mart_depassements_reels",
              "mart_monopoles_cpv", "ref_groupes_esn"]:
    try:
        df = src.execute("SELECT * FROM main." + table).fetchdf()
        prod.execute("CREATE TABLE " + table + " AS SELECT * FROM df")
        print(table + " : " + str(len(df)) + " lignes")
    except Exception as e:
        print(table + " ERREUR : " + str(e))

src.close()
prod.close()