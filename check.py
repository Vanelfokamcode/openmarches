import duckdb, pandas as pd

src = duckdb.connect("data/openmarches.duckdb")
prod = duckdb.connect("data/openmarches_prod.duckdb")

# Voir ce qui manque
print("Tables dans prod :")
print(prod.execute("SHOW TABLES").fetchdf().to_string())

# Ajouter stg_titulaires en version allegee
df = src.execute("""
    SELECT marche_id, groupe, siret, nom_entreprise,
        montant_eur, annee, code_cpv, famille_cpv,
        LEFT(objet, 150) as objet, acheteur_siret,
        flag_montant_suspect, flag_date_suspecte
    FROM main.stg_titulaires
    WHERE flag_montant_suspect = FALSE
    LIMIT 50000
""").fetchdf()
prod.execute("CREATE OR REPLACE TABLE stg_titulaires AS SELECT * FROM df")
print("stg_titulaires : " + str(len(df)) + " lignes")

src.close()
prod.close()