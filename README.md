# OpenMarchés

> L'argent public IT en France — ouvert, analysé, visualisé.

Depuis 2019, chaque marché public de plus de 25 000€ doit être publié en open data.
Ces données existent. Personne ne les avait rendues lisibles. Jusqu'ici.

## Découvertes clés

- **SCC France : 13,3 milliards d'euros** de marchés IT publics — le géant invisible que personne ne mentionne, dominant sur les logiciels (55%) et le matériel informatique (16%)
- **239 900% de dépassement de budget** — un marché signé à 125 000€ converti en 300 millions via avenant. 13 474 marchés avec dépassements réels identifiés.
- **48% des marchés IT vont à des PME indépendantes** — contre toute intuition, le marché n'est pas dominé par les grandes ESN nationales

## Stack technique

| Couche | Technologie |
|--------|-------------|
| Ingestion | Python · DuckDB · JSON |
| Transformation | dbt · DuckDB |
| Orchestration | Dagster |
| API | FastAPI |
| Frontend | HTML · JavaScript vanilla |

## Architecture

```
DECP JSON (634Mo)
    └── DuckDB (raw_marches_full)
            └── dbt staging (stg_marches, stg_titulaires)
                    └── dbt marts (classement, anomalies, dépassements, monopoles, geo)
                            └── FastAPI :8002
                                    └── Frontend HTML
```

## Lancer en local

```bash
git clone https://github.com/Vanelfokamcode/openmarches
cd openmarches
python3 -m venv venv && source venv/bin/activate
pip install duckdb dagster fastapi uvicorn dbt-duckdb requests pandas

# Télécharger les données
wget -O data/raw/decp_full.json \
  "https://www.data.gouv.fr/api/1/datasets/r/bd33e98f-f8e3-49ba-9f26-51c95fe57234"

# Charger + transformer
python3 ingestion/load_decp_full.py
cd transform/openmarches && dbt seed && dbt run && cd ../..

# Lancer l'API
uvicorn api.main:app --port 8002

# Ouvrir le frontend
cd frontend && python3 -m http.server 3000
```

## Source des données

- **DECP consolidé** — Ministère des Finances, data.gouv.fr, licence ouverte
- Mis à jour mensuellement
- Couvre tous les marchés publics français > 25 000€ depuis 2018

## Limites documentées

- Les montants DECP pour les accords-cadres représentent des plafonds, pas des dépenses réelles
- Le champ `lieuExecution` n'est pas uniforme — les marchés nationaux sont souvent domiciliés à Paris
- Environ 13% des marchés IT restent non mappés dans le référentiel des groupes

---
*Projet personnel · Vanel Fokam · 2026 · github.com/Vanelfokamcode*