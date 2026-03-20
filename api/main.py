from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from api.database import query

app = FastAPI(
    title="OpenMarchés API",
    description="Marchés publics IT France — données DECP consolidées",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/classement", summary="Classement des groupes ESN par volume IT")
def classement(
    annee: str = Query(None, description="Filtrer par année ex: 2024"),
    limit: int = Query(15, le=50)
):
    where = "WHERE annee = ?" if annee else ""
    params = [annee] if annee else None
    sql = f"""
        SELECT groupe, annee,
            SUM(nb_marches) as nb_marches,
            ROUND(SUM(total_millions_eur), 1) as total_M_eur,
            ROUND(AVG(part_marche_pct), 1) as part_marche_pct
        FROM main.mart_classement_groupes
        {where}
        GROUP BY groupe, annee
        ORDER BY total_M_eur DESC
        LIMIT {limit}
    """
    return query(sql, params)


@app.get("/anomalies", summary="Marchés avec données suspectes")
def anomalies(
    type_anomalie: str = Query(None, description="MONTANT_SUSPECT ou DATE_SUSPECTE"),
    limit: int = Query(50, le=200)
):
    where = "WHERE type_anomalie = ?" if type_anomalie else ""
    params = [type_anomalie] if type_anomalie else None
    sql = f"""
        SELECT type_anomalie, marche_id, acheteur_siret,
            montant_eur, date_notification, annee,
            LEFT(objet, 100) as objet_court,
            description
        FROM main.mart_anomalies
        {where}
        ORDER BY type_anomalie, annee
        LIMIT {limit}
    """
    return query(sql, params)


@app.get("/acheteurs", summary="Top acheteurs publics par volume IT")
def acheteurs(
    profil: str = Query(None, description="REGULIER, OCCASIONNEL ou PONCTUEL"),
    limit: int = Query(20, le=100)
):
    where = "WHERE profil_publication = ? AND nb_marches_it > 0" if profil else "WHERE nb_marches_it > 0"
    params = [profil] if profil else None
    sql = f"""
        SELECT acheteur_siret, nb_marches_total, nb_marches_it,
            nb_annees_actives, total_millions_it,
            premiere_annee, derniere_annee, profil_publication
        FROM main.mart_acheteurs_actifs
        {where}
        ORDER BY total_millions_it DESC
        LIMIT {limit}
    """
    return query(sql, params)


@app.get("/search", summary="Recherche libre dans les marchés")
def search(
    q: str = Query(..., min_length=2, description="Terme de recherche"),
    limit: int = Query(20, le=100)
):
    sql = """
        SELECT marche_id, groupe, siret, nom_entreprise,
            montant_eur, annee, code_cpv,
            LEFT(objet, 150) as objet,
            acheteur_siret
        FROM main.stg_titulaires
        WHERE (LOWER(objet) LIKE LOWER(CONCAT('%', ?, '%'))
            OR LOWER(groupe) LIKE LOWER(CONCAT('%', ?, '%'))
            OR LOWER(nom_entreprise) LIKE LOWER(CONCAT('%', ?, '%')))
        AND flag_montant_suspect = FALSE
        ORDER BY montant_eur DESC
        LIMIT ?
    """
    return query(sql, [q, q, q, limit])


@app.get("/stats", summary="Statistiques globales du dataset")
def stats():
    sql = """
        SELECT
            COUNT(*) as total_marches,
            COUNT(DISTINCT acheteur_siret) as nb_acheteurs,
            COUNT(DISTINCT siret) as nb_titulaires,
            ROUND(SUM(montant_eur)/1e9, 2) as total_milliards_eur,
            MIN(annee) as premiere_annee,
            MAX(annee) as derniere_annee
        FROM main.stg_titulaires
        WHERE flag_montant_suspect = FALSE
        AND flag_date_suspecte = FALSE
    """
    return query(sql)[0]


@app.get("/depassements", summary="Marchés avec dépassements de budget réels")
def depassements(
    min_pct: float = Query(100.0, description="Dépassement minimum en % (défaut 100%)"),
    limit: int = Query(30, le=200)
):
    sql = f"""
        SELECT
            marche_id,
            acheteur_siret,
            titulaire_siret,
            ROUND(montant_initial_eur, 0) as montant_initial_eur,
            ROUND(montant_apres_modif_eur, 0) as montant_apres_modif_eur,
            ROUND(depassement_eur, 0) as depassement_eur,
            depassement_pct,
            LEFT(objet, 120) as objet,
            date_notification,
            date_modif
        FROM main.mart_depassements_reels
        WHERE depassement_pct >= {min_pct}
        ORDER BY depassement_pct DESC
        LIMIT {limit}
    """
    return query(sql)


@app.get("/depassements/stats", summary="Statistiques globales des dépassements")
def depassements_stats():
    sql = """
        SELECT
            COUNT(*) as nb_marches_depasses,
            ROUND(AVG(depassement_pct), 1) as depassement_moyen_pct,
            ROUND(MEDIAN(depassement_pct), 1) as depassement_median_pct,
            MAX(depassement_pct) as depassement_max_pct,
            ROUND(SUM(depassement_eur) / 1e6, 1) as total_depassement_millions_eur
        FROM main.mart_depassements_reels
        WHERE depassement_pct < 500000
    """
    return query(sql)[0]
