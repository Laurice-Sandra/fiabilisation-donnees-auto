"""Fonctions de contrôle de qualité réutilisables (notebook ou futur pipeline).

Chaque fonction compte un nombre de lignes en anomalie et ne contient aucun
seuil en dur : les seuils viennent toujours de `config/parametres.yaml`
(ENF1), on se contente de les passer en paramètre ici.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


"""Fonctions de contrôle de qualité réutilisables (notebook ou futur pipeline).

Chaque fonction compte un nombre de lignes en anomalie et ne contient aucun
seuil en dur : les seuils viennent toujours de `config/parametres.yaml`
(ENF1), on se contente de les passer en paramètre ici.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class AnomalieBloquante(Exception):
    """Levée quand un contrôle de niveau "bloquant" trouve au moins une
    anomalie. Le code appelant (futur src/pipeline.py) peut laisser cette
    exception remonter pour arrêter le traitement (EF2 : "bloquant = arrêt").
    """


def verifier_niveau(nom_controle: str, nb_anomalies: int, niveau: str) -> None:
    """Applique le niveau d'un contrôle (EF2) :
    - "bloquant" + au moins 1 anomalie -> lève AnomalieBloquante (arrêt).
    - "alerte" (ou "bloquant" avec 0 anomalie) -> ne fait rien ici ; c'est au
      futur src/rapport.py de journaliser l'anomalie pour le reporting.
    """
    if niveau == "bloquant" and nb_anomalies > 0:
        raise AnomalieBloquante(
            f"Contrôle bloquant '{nom_controle}' : {nb_anomalies} anomalie(s) détectée(s), arrêt du traitement."
        )


class Volumetrie:
    """Trace les volumétries (nb de lignes, sommes de colonnes) à chaque
    étape du traitement (EF4 : "volumétries et totaux comparés avant et
    après chaque étape"). Un seul appel à `enregistrer` par étape suffit :
    pas besoin de répéter le même code de comptage partout dans le pipeline.
    """

    def __init__(self) -> None:
        self._etapes: list[dict] = []

    def enregistrer(self, etape: str, df: DataFrame, colonnes_sommees: tuple = ()) -> None:
        ligne = {"etape": etape, "nb_lignes": df.count()}
        for colonne in colonnes_sommees:
            ligne[f"somme_{colonne}"] = df.agg(F.sum(colonne)).first()[0] or 0.0
        self._etapes.append(ligne)

    def tableau(self) -> list[dict]:
        """Renvoie les volumétries enregistrées, dans l'ordre, prêtes à être
        affichées (print, pandas.DataFrame) ou réutilisées par le futur
        reporting HTML.
        """
        return list(self._etapes)


def compter_doublons(df: DataFrame, cle: str) -> int:
    """Nombre de valeurs de `cle` qui apparaissent plus d'une fois dans df."""
    return df.groupBy(cle).count().filter(F.col("count") > 1).count()


def compter_lignes_dupliquees(df: DataFrame) -> int:
    """Nombre de lignes en trop quand on ne garde que des lignes distinctes
    (toutes colonnes identiques). Différent de `compter_doublons`, qui lui
    ne regarde qu'une seule colonne-clé.
    """
    return df.count() - df.distinct().count()


def compter_cles_orphelines(df_source: DataFrame, df_reference: DataFrame, cle: str) -> int:
    """Nombre de lignes de df_source dont `cle` n'existe dans aucune ligne
    de df_reference (ex: sinistre dont l'IDpol n'a pas de contrat).
    """
    cles_valides = df_reference.select(cle).distinct()
    return df_source.join(cles_valides, on=cle, how="left_anti").count()


def compter_hors_bornes(
    df: DataFrame, colonne: str, borne_min: float | None = None, borne_max: float | None = None
) -> int:
    """Nombre de lignes dont `colonne` est strictement en dehors de
    [borne_min, borne_max]. Une borne à None n'est pas contrôlée.
    """
    condition = F.lit(False)
    if borne_min is not None:
        condition = condition | (F.col(colonne) < borne_min)
    if borne_max is not None:
        condition = condition | (F.col(colonne) > borne_max)
    return df.filter(condition).count()


def compter_valeurs_egales(df: DataFrame, colonne: str, valeur: float) -> int:
    """Nombre de lignes où `colonne` vaut exactement `valeur`.

    Utile pour un cas comme ClaimAmount == 0 : un montant négatif (recours /
    subrogation) est un cas métier normal, donc pas une anomalie, mais un
    montant de sinistre exactement nul est suspect (sinistre sans coût réel).
    """
    return df.filter(F.col(colonne) == valeur).count()


def compter_incoherence_nb_sinistres(
    df_contrats: DataFrame, df_sinistres: DataFrame, cle: str, colonne_claimnb: str
) -> int:
    """Nombre de contrats dont `colonne_claimnb` ne correspond pas au nombre
    réel de lignes de ce contrat dans df_sinistres (un contrat sans sinistre
    compte pour 0, pas pour une valeur manquante).
    """
    nb_reel = df_sinistres.groupBy(cle).count().withColumnRenamed("count", "nb_reel")
    jointure = df_contrats.select(cle, colonne_claimnb).join(nb_reel, on=cle, how="left")
    jointure = jointure.fillna({"nb_reel": 0})
    return jointure.filter(F.col(colonne_claimnb) != F.col("nb_reel")).count()
