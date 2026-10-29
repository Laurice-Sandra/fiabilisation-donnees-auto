"""Construction de la base contrats x sinistres jointe (jointure.type=left).

Aucune fonction ici ne supprime ou ne corrige une valeur : on respecte la
règle du cahier des charges ("on signale et on documente, on ne répare pas
sans règle écrite"). Les écarts (ex: ClaimNb vs nb_sinistres_reel) restent
visibles comme des colonnes de la base finale, pour que le reporting puisse
les expliquer plutôt que les cacher.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agreger_sinistres(df_sinistres: DataFrame, cle: str) -> DataFrame:
    """Ramène df_sinistres à une ligne par `cle` (clé de jointure unique).

    Chaque ligne de sinistre compte pour 1 sinistre, y compris les lignes
    identiques (IDpol + ClaimAmount) : on ne les traite pas comme des
    doublons à supprimer (décision prise avec Laurice : l'explication peut
    être légitime, ex. paiement fractionné). `montant_total_sinistres` est
    la somme des montants (un recours/subrogation négatif réduit bien le
    total).
    """
    return (
        df_sinistres.withColumn("_un_sinistre", F.lit(1))
        .groupBy(cle)
        .agg(
            F.sum("ClaimAmount").alias("montant_total_sinistres"),
            F.sum("_un_sinistre").alias("nb_sinistres_reel"),
        )
    )


def isoler_orphelins(df_sinistres: DataFrame, df_contrats: DataFrame, cle: str) -> DataFrame:
    """Lignes de sinistres (détail complet, non agrégé) dont `cle` n'a pas
    de contrat correspondant. On les isole dans une base à part plutôt que
    de les perdre : elles pourront être réinjectées lors du calage
    tarifaire dans les projets suivants (GLM fréquence/coût moyen).
    """
    cles_valides = df_contrats.select(cle).distinct()
    return df_sinistres.join(cles_valides, on=cle, how="left_anti")


def joindre_contrats_sinistres(df_contrats: DataFrame, df_sinistres: DataFrame, cle: str) -> DataFrame:
    """Base finale : LEFT JOIN contrats <- sinistres agrégés par clé unique.

    Un contrat sans sinistre obtient nb_sinistres_reel=0 et
    montant_total_sinistres=0 : ce n'est pas une correction de donnée, c'est
    la conséquence normale du LEFT JOIN (un contrat réellement sans sinistre
    a bien 0 sinistre et 0 € de charge).

    Ajoute aussi `nb_sinistres_coherent` (booléen) : True si ClaimNb ==
    nb_sinistres_reel. On ne supprime ni ne corrige rien, on signale
    seulement (cf. hypothèse vérifiée sur toutes les données : ClaimNb >=
    nb_sinistres_reel toujours, jamais l'inverse — nb_sinistres_reel semble
    être le sous-ensemble des sinistres dont le montant est déjà connu).
    Les projets suivants (GLM fréquence) peuvent filtrer sur cette colonne
    s'ils veulent ne garder que les lignes jugées fiables.
    """
    sinistres_agreges = agreger_sinistres(df_sinistres, cle)
    jointe = df_contrats.join(sinistres_agreges, on=cle, how="left")
    jointe = jointe.fillna({"nb_sinistres_reel": 0, "montant_total_sinistres": 0.0})
    return jointe.withColumn("nb_sinistres_coherent", F.col("ClaimNb") == F.col("nb_sinistres_reel"))


def calculer_ecart_nb_sinistres(df_jointe: DataFrame, colonne_claimnb: str = "ClaimNb") -> dict:
    """Mesure l'écart entre ClaimNb (déclaré côté contrat) et
    nb_sinistres_reel (recalculé côté sinistres), après jointure :

    - nb_contrats_ecart : nombre de contrats où les deux valeurs diffèrent.
    - montant_concerne : somme de montant_total_sinistres sur ces contrats
      (l'impact financier de l'écart, pas juste un nombre de lignes).
    - nb_contrats_claimnb_sans_sinistre : cas particulier à part, plus
      suspect qu'un simple écart de comptage : le contrat déclare des
      sinistres (ClaimNb > 0) mais aucun n'a été retrouvé côté sinistres
      après la jointure (nb_sinistres_reel == 0).
    """
    ecart = df_jointe.filter(F.col(colonne_claimnb) != F.col("nb_sinistres_reel"))
    absence_totale = df_jointe.filter(
        (F.col(colonne_claimnb) > 0) & (F.col("nb_sinistres_reel") == 0)
    )
    return {
        "nb_contrats_ecart": ecart.count(),
        "montant_concerne": ecart.agg(F.sum("montant_total_sinistres")).first()[0] or 0.0,
        "nb_contrats_claimnb_sans_sinistre": absence_totale.count(),
    }


def detailler_ecart_nb_sinistres(df_jointe: DataFrame, colonne_claimnb: str = "ClaimNb") -> DataFrame:
    """Détaille l'écart nb_sinistres_reel - ClaimNb, contrairement à
    `calculer_ecart_nb_sinistres` qui ne renvoie qu'un total global (masquant
    les cas rares derrière une moyenne écrasée par les contrats à 0 €).

    Une ligne par valeur d'écart (ex: -1, +1, +2...), avec :
    - nb_contrats : combien de contrats ont cet écart précis.
    - montant_moyen : la moyenne de montant_total_sinistres UNIQUEMENT sur
      ces contrats-là (donc pas diluée par les milliers de contrats sans
      écart ni par les contrats en écart "absence totale" à 0 €, qui
      apparaissent dans leur propre ligne d'écart négatif).
    """
    return (
        df_jointe.withColumn("ecart_nb_sinistres", F.col("nb_sinistres_reel") - F.col(colonne_claimnb))
        .filter(F.col("ecart_nb_sinistres") != 0)
        .groupBy("ecart_nb_sinistres")
        .agg(
            F.count("*").alias("nb_contrats"),
            F.avg("montant_total_sinistres").alias("montant_moyen"),
        )
        .orderBy("ecart_nb_sinistres")
    )

