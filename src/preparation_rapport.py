"""Préparation des données pour le reporting HTML (src/rapport.py).

Ces fonctions ne font aucun rendu (Plotly, HTML) : elles renvoient des
DataFrames pandas simples, faciles à tester sans dépendance réseau ni
navigateur (ENF3). La construction du HTML lui-même vit dans
`construire_figures.py` et `rapport.py` (génération).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculer_kpis_globaux(df: DataFrame) -> dict:
    """KPIs de synthèse du portefeuille entier (niveau le plus global)."""
    ligne = (
        df.agg(
            F.count("*").alias("nb_contrats"),
            F.sum("Exposure").alias("exposition_totale"),
            F.sum("ClaimNb").alias("claimnb_total"),
            F.sum("nb_sinistres_reel").alias("nb_sinistres_reel_total"),
            F.sum("montant_total_sinistres").alias("charge_totale"),
        )
        .toPandas()
        .iloc[0]
        .to_dict()
    )
    ligne["frequence_globale"] = ligne["nb_sinistres_reel_total"] / ligne["exposition_totale"]
    ligne["cout_moyen_global"] = (
        ligne["charge_totale"] / ligne["nb_sinistres_reel_total"]
        if ligne["nb_sinistres_reel_total"] else None
    )
    ligne["prime_pure_globale"] = (
        ligne["frequence_globale"] * ligne["cout_moyen_global"]
        if ligne["cout_moyen_global"] is not None else None
    )
    return ligne


def agreger_par_segment(df: DataFrame, *colonnes: str):
    """Agrège nb_contrats/exposition/sinistres/charge par une ou plusieurs
    colonnes (ex: classe_DrivAge, classe_BonusMalus). Renvoie un pandas
    DataFrame avec frequence et cout_moyen calculés.
    """
    agrege = (
        df.groupBy(*colonnes)
        .agg(
            F.count("*").alias("nb_contrats"),
            F.sum("Exposure").alias("exposition_totale"),
            F.sum("nb_sinistres_reel").alias("nb_sinistres_reel"),
            F.sum("montant_total_sinistres").alias("montant_total_sinistres"),
        )
        .toPandas()
    )
    agrege["frequence"] = agrege["nb_sinistres_reel"] / agrege["exposition_totale"]
    agrege["cout_moyen"] = agrege["montant_total_sinistres"] / agrege["nb_sinistres_reel"].replace(0, float("nan"))
    return agrege


def agreger_par_region(df: DataFrame):
    """Agrège par Region, avec une colonne `code` (sans le préfixe "R") prête
    pour la jointure avec le GeoJSON des régions.
    """
    agrege = agreger_par_segment(df, "Region")
    agrege["code"] = agrege["Region"].str.replace("R", "", regex=False)
    return agrege
