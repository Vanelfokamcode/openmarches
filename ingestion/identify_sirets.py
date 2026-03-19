import duckdb, requests, json, time
from pathlib import Path

conn = duckdb.connect("data/openmarches.duckdb")

# Tous les SIRET uniques sur marchés IT
sirets = conn.execute("""
    SELECT DISTINCT titulaires[1]['titulaire']['id'] AS siret,
        COUNT(*) as nb, ROUND(SUM(montant)/1e6,1) as total_M
    FROM raw_marches
    WHERE codeCPV LIKE '72%'
    AND montant IS NOT NULL AND montant < 1e10
    AND titulaires[1]['titulaire']['id'] IS NOT NULL
    GROUP BY siret
    ORDER BY total_M DESC
    LIMIT 100
""").fetchdf()

print(f"SIRET uniques à identifier : {len(sirets)}")

results = []
for _, row in sirets.iterrows():
    siret = row["siret"]
    try:
        r = requests.get(
            f"https://recherche-entreprises.api.gouv.fr/search?q={siret}&page=1&per_page=1",
            timeout=5
        )
        if r.status_code == 200:
            data = r.json().get("results", [])
            nom = data[0].get("nom_complet", "INCONNU") if data else "INCONNU"
        else:
            nom = f"HTTP_{r.status_code}"
    except Exception as e:
        nom = "ERREUR"
    results.append({"siret": siret, "nom": nom, "nb_marches": int(row["nb"]), "total_M": float(row["total_M"])})
    time.sleep(0.3)  # rate limit

# Sauvegarder
Path("data").mkdir(exist_ok=True)
with open("data/sirets_resolus.json", "w") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print("Résultats sauvegardés dans data/sirets_resolus.json")
for r in results[:20]:
    print(f"  {r['siret']} → {r['nom']:<50} {r['total_M']:>8.1f} M€")
conn.close()
