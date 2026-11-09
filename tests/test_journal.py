"""Tests de src/journal.py (petits DataFrames Spark en mémoire, ENF3)."""

from src.journal import (
    COLONNES_JOURNAL,
    construire_journal,
    lister_cles_orphelines,
    lister_doublons,
    lister_hors_bornes,
    lister_incoherence_nb_sinistres,
    lister_lignes_dupliquees,
)


def test_lister_doublons(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["IDpol"])
    resultat = lister_doublons(df, "IDpol")
    assert list(resultat["identifiant"]) == [1]
    assert list(resultat["valeur_observee"]) == [2]


def test_lister_lignes_dupliquees(spark):
    df = spark.createDataFrame(
        [(1, 100.0), (1, 100.0), (2, 50.0)], ["IDpol", "ClaimAmount"]
    )
    resultat = lister_lignes_dupliquees(df, "IDpol", ["IDpol", "ClaimAmount"])
    assert list(resultat["identifiant"]) == [1]
    assert list(resultat["valeur_observee"]) == [2]


def test_lister_cles_orphelines(spark):
    contrats = spark.createDataFrame([(1,), (2,)], ["IDpol"])
    sinistres = spark.createDataFrame([(1, 10.0), (3, 20.0)], ["IDpol", "ClaimAmount"])
    resultat = lister_cles_orphelines(sinistres, contrats, "IDpol", "ClaimAmount")
    assert list(resultat["identifiant"]) == [3]
    assert list(resultat["valeur_observee"]) == [20.0]


def test_lister_hors_bornes(spark):
    df = spark.createDataFrame([(1, 0.5), (2, 1.5), (3, -0.1)], ["IDpol", "Exposure"])
    resultat = lister_hors_bornes(df, "IDpol", "Exposure", borne_min=0, borne_max=1)
    assert set(resultat["identifiant"]) == {2, 3}


def test_lister_incoherence_nb_sinistres(spark):
    df_jointe = spark.createDataFrame(
        [(1, 1, 1), (2, 2, 0)], ["IDpol", "ClaimNb", "nb_sinistres_reel"]
    )
    resultat = lister_incoherence_nb_sinistres(df_jointe, "IDpol")
    assert list(resultat["identifiant"]) == [2]
    assert resultat["valeur_observee"].iloc[0] == "ClaimNb=2;nb_sinistres_reel=0"


def test_construire_journal_colonnes_et_contenu(spark):
    df1 = lister_doublons(spark.createDataFrame([(1,), (1,)], ["IDpol"]), "IDpol")
    df2 = spark.createDataFrame([], "identifiant int, valeur_observee int").toPandas()
    journal = construire_journal(
        {"doublons_contrats": df1, "cles_orphelines": df2},
        {"doublons_contrats": "bloquant", "cles_orphelines": "alerte"},
    )
    assert list(journal.columns) == COLONNES_JOURNAL
    assert len(journal) == 1  # df2 est vide, exclu
    assert journal.iloc[0]["controle"] == "doublons_contrats"
    assert journal.iloc[0]["niveau"] == "bloquant"


def test_construire_journal_vide_si_aucune_anomalie():
    journal = construire_journal({}, {})
    assert journal.empty
    assert list(journal.columns) == COLONNES_JOURNAL
