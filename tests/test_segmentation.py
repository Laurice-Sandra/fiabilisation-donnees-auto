"""Tests de src/segmentation.py (petits DataFrames Spark en mémoire, ENF3)."""

from src.segmentation import ajouter_classe


def test_ajouter_classe_cree_des_classes_croissantes(spark):
    df = spark.createDataFrame([(i,) for i in range(1, 101)], ["age"])
    df_classe, etiquettes = ajouter_classe(df, "age", nb_classes=4)
    assert len(etiquettes) == 4
    # la premiere etiquette doit etre "< X", la derniere ">= Y"
    assert etiquettes[0].startswith("< ")
    assert etiquettes[-1].startswith(">= ")
    # toutes les lignes ont recu une classe (aucune valeur nulle)
    assert df_classe.filter(df_classe[f"classe_age"].isNull()).count() == 0


def test_ajouter_classe_respecte_les_bornes(spark):
    df = spark.createDataFrame([(10,), (20,), (30,), (40,)], ["valeur"])
    df_classe, etiquettes = ajouter_classe(df, "valeur", nb_classes=2)
    # 2 classes : la plus petite moitie et la plus grande
    resultat = {row["valeur"]: row["classe_valeur"] for row in df_classe.collect()}
    assert resultat[10] == etiquettes[0]
    assert resultat[40] == etiquettes[-1]


def test_ajouter_classe_variable_concentree_donne_moins_de_classes(spark):
    # 90% des valeurs sont identiques (50) : les quantiles se chevauchent,
    # on doit obtenir moins de classes que demande (comme BonusMalus dans
    # les vraies donnees, concentre sur le plancher legal).
    valeurs = [(50,)] * 90 + [(100,)] * 5 + [(150,)] * 5
    df = spark.createDataFrame(valeurs, ["bm"])
    _, etiquettes = ajouter_classe(df, "bm", nb_classes=5)
    assert len(etiquettes) < 5
