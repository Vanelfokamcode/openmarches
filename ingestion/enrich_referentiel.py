import json, csv
from pathlib import Path

with open("data/sirets_resolus.json") as f:
    sirets = json.load(f)

# Mapping manuel groupe par mots-cles dans le nom
MAPPING_GROUPES = {
    "CAPGEMINI": "CAPGEMINI", "SOGETI": "CAPGEMINI", "ALTRAN": "CAPGEMINI",
    "SOPRA": "SOPRA STERIA", "STERIA": "SOPRA STERIA",
    "ACCENTURE": "ACCENTURE",
    "IBM": "IBM",
    "ORANGE": "ORANGE",
    "ATOS": "ATOS", "BULL": "ATOS", "WORLDLINE": "ATOS",
    "CGI": "CGI",
    "UMANIS": "UMANIS",
    "ERNST": "EY", "EY ": "EY",
    "DELOITTE": "DELOITTE",
    "KPMG": "KPMG",
    "WAVESTONE": "WAVESTONE",
    "BEARINGPOINT": "BEARINGPOINT",
    "DEVOTEAM": "DEVOTEAM",
    "ECONOCOM": "ECONOCOM",
}

def detecter_groupe(nom):
    nom_upper = nom.upper()
    for mot, groupe in MAPPING_GROUPES.items():
        if mot in nom_upper:
            return groupe
    return "INDEPENDANT"

# Generer le CSV enrichi
rows = []
for s in sirets:
    groupe = detecter_groupe(s["nom"])
    rows.append({
        "siret": s["siret"],
        "nom_entreprise": s["nom"],
        "groupe": groupe,
        "nb_marches_it": s["nb_marches"],
        "total_M_eur": s["total_M"],
        "source": "api_auto"
    })

with open("transform/seeds/ref_groupes_esn.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["siret","nom_entreprise","groupe","nb_marches_it","total_M_eur","source"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Référentiel enrichi : {len(rows)} SIRET")
print("\nDistribution par groupe :")
from collections import Counter
groupes = Counter(r["groupe"] for r in rows)
for g, n in groupes.most_common(15):
    total = sum(r["total_M_eur"] for r in rows if r["groupe"] == g)
    print(f"  {g:<25} {n:>3} entités   {total:>8.1f} M€")
