import duckdb, requests, json

conn = duckdb.connect("data/openmarches.duckdb")

def identify(siret):
    try:
        r = requests.get(
            f"https://recherche-entreprises.api.gouv.fr/search?q={siret}&page=1&per_page=1",
            timeout=5
        )
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                return results[0].get("nom_complet", "?")
    except:
        pass
    return "?"

print("=== TOP 10 MARCHÉS IT — ENTREPRISES IDENTIFIÉES ===")
top = conn.execute("""
    SELECT
        titulaires[1]['titulaire']['id'] AS siret,
        COUNT(*) AS nb_marches,
        ROUND(SUM(montant) / 1e6, 1) AS total_M_eur
    FROM raw_marches
    WHERE codeCPV LIKE '72%'
    AND montant IS NOT NULL AND montant < 1e10
    GROUP BY siret
    ORDER BY total_M_eur DESC
    LIMIT 10
""").fetchdf()

for _, row in top.iterrows():
    nom = identify(row["siret"])
    print(f"{nom:<45} {row['total_M_eur']:>8.1f} M€  ({int(row['nb_marches'])} marchés)")

conn.close()
