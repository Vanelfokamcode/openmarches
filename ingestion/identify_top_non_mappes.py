import duckdb, pandas as pd, requests, time, json, csv

conn = duckdb.connect("data/openmarches.duckdb")

top = conn.execute("""
    WITH siret_ext AS (
        SELECT titulaires[1]['titulaire']['id'] AS siret, montant
        FROM raw_marches_full
        WHERE codeCPV LIKE '72%'
        AND montant IS NOT NULL AND montant < 500000000
    )
    SELECT s.siret, COUNT(*) as nb, ROUND(SUM(s.montant)/1e6,1) as total_M
    FROM siret_ext s
    LEFT JOIN ref_groupes_esn g ON s.siret = g.siret
    WHERE g.siret IS NULL AND s.siret IS NOT NULL
    GROUP BY s.siret
    HAVING SUM(s.montant)/1e6 > 10
    ORDER BY total_M DESC
""").fetchdf()

print(f"SIRET > 10M a identifier : {len(top)}")
print(f"Volume total : {top['total_M'].sum():.0f} M€")

MAPPING = {
    "CAPGEMINI":"CAPGEMINI","SOGETI":"CAPGEMINI","ALTRAN":"CAPGEMINI",
    "SOPRA":"SOPRA STERIA","STERIA":"SOPRA STERIA","ACCENTURE":"ACCENTURE",
    "IBM":"IBM","ORANGE":"ORANGE","ATOS":"ATOS","BULL":"ATOS",
    "CGI":"CGI","INETUM":"INETUM","SFR":"SFR","BOUYGUES":"BOUYGUES",
    "THALES":"THALES","AIRBUS":"AIRBUS","DASSAULT":"DASSAULT",
    "EY ":"EY","ERNST":"EY","DELOITTE":"DELOITTE","KPMG":"KPMG",
    "WAVESTONE":"WAVESTONE","DEVOTEAM":"DEVOTEAM","ECONOCOM":"ECONOCOM",
    "COMPUTACENTER":"COMPUTACENTER","MC2I":"MC2I","DOCAPOSTE":"DOCAPOSTE",
    "INTRINSEC":"INTRINSEC","STELOGY":"STELOGY","MALT":"MALT",
    "RICOH":"RICOH","CHEOPS":"CHEOPS","ADVENS":"ADVENS","LINKT":"LINKT",
    "OPEN ":"OPEN","HEXAGONE":"HEXAGONE DIGITALE","CRAYON":"CRAYON",
    "AXIANS":"AXIANS","ECONOCOM":"ECONOCOM","MAGELLAN":"MAGELLAN",
    "AUBAY":"AUBAY","MICROPOLE":"MICROPOLE","NEXEO":"NEXEO",
}

def get_groupe(nom):
    for k,v in MAPPING.items():
        if k in nom.upper(): return v
    return "INDEPENDANT"

existants = set(pd.read_csv("transform/seeds/ref_groupes_esn.csv", dtype=str)["siret"].tolist())
resultats = []

for i, (_, row) in enumerate(top.iterrows()):
    siret = str(row["siret"]).strip()
    if siret in existants:
        continue
    try:
        r = requests.get(
            f"https://recherche-entreprises.api.gouv.fr/search?q={siret}&page=1&per_page=1",
            timeout=5)
        nom = r.json().get("results",[{}])[0].get("nom_complet","INCONNU") if r.status_code==200 else "INCONNU"
    except: nom = "INCONNU"
    groupe = get_groupe(nom)
    resultats.append({"siret":siret,"nom_entreprise":nom,"groupe":groupe,"total_M":float(row["total_M"])})
    print(f"  {siret}  {row['total_M']:>7.1f} M€  {nom[:45]:<45} → {groupe}")
    time.sleep(0.3)

with open("data/top_non_mappes_identifies.json","w") as f:
    json.dump(resultats, f, indent=2, ensure_ascii=False)

with open("transform/seeds/ref_groupes_esn.csv","a",newline="") as f:
    w = csv.writer(f)
    for e in resultats:
        w.writerow([e["siret"],e["nom_entreprise"],e["groupe"]])

print(f"\n{len(resultats)} nouveaux SIRET ajoutes")
conn.close()
