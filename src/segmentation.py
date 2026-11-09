"""Segmentation de variables continues en classes lisibles.

Réutilisé par `notebooks/analyse_descriptive.ipynb` et `src/rapport.py` :
avant, cette logique était dupliquée dans le notebook uniquement.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ajouter_classe(df: DataFrame, colonne: str, nb_classes: int) -> tuple[DataFrame, list[str]]:
    """Ajoute une colonne `classe_{colonne}` à `df`, découpée en `nb_classes`
    classes par quantiles (pas par largeur égale : évite une classe géante
    et plusieurs classes vides sur des variables asymétriques comme Density).

    Renvoie (df_avec_classe, etiquettes) où `etiquettes` est la liste des
    noms de classe dans l'ordre croissant (pas alphabétique : "< 32" doit
    rester avant "[32, 40[", qui doit rester avant ">= 57"). Cet ordre doit
    être réutilisé explicitement pour trier ou afficher les classes, car un
    tri par défaut (alphabétique) les mélangerait.
    """
    quantiles = [i / nb_classes for i in range(nb_classes + 1)]
    bornes = sorted(set(df.approxQuantile(colonne, quantiles, 0.01)))

    def etiquette(i):
        if i == 0:
            return f"< {bornes[1]:.0f}"
        if i == len(bornes) - 2:
            return f">= {bornes[i]:.0f}"
        return f"[{bornes[i]:.0f}, {bornes[i+1]:.0f}["

    etiquettes = [etiquette(i) for i in range(len(bornes) - 1)]

    expr = F.lit(etiquettes[-1])
    for i in range(len(bornes) - 2, -1, -1):
        expr = F.when(F.col(colonne) < bornes[i + 1], F.lit(etiquettes[i])).otherwise(expr)
    return df.withColumn(f"classe_{colonne}", expr), etiquettes
