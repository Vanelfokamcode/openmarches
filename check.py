import duckdb, pandas as pd

conn = duckdb.connect("data/openmarches.duckdb")
ref = pd.read_csv("transform/seeds/ref_groupes_esn.csv", dtype=str)
conn.execute("CREATE OR REPLACE TABLE ref_groupes_esn AS SELECT * FROM ref")
print("Referentiel : " + str(len(ref)))

result = conn.execute("""
    WITH siret_ext AS (
        SELECT titulaires[1]['titulaire']['id'] AS siret, montant
        FROM raw_marches_full
        WHERE codeCPV LIKE '72%'
        AND montant IS NOT NULL AND montant < 500000000
    )
    SELECT COALESCE(g.groupe, 'NON MAPPE') as groupe,
        COUNT(*) as nb,
        ROUND(SUM(s.montant)/1e6,1) as total_M
    FROM siret_ext s
    LEFT JOIN ref_groupes_esn g ON s.siret = g.siret
    GROUP BY groupe
    ORDER BY total_M DESC
    LIMIT 20
""").fetchdf()

total = result["total_M"].sum()
for _, row in result.iterrows():
    pct = round(row["total_M"] / total * 100, 1)
    print(str(row["groupe"]).ljust(25) + str(pct).rjust(6) + "pct  " + str(int(row["total_M"])) + " M")

conn.close()