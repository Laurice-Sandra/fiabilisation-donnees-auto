"""Tests des fonctions de contrôle de src/controles.py.

Chaque test construit un petit DataFrame Spark en mémoire : pas de
dépendance aux vraies données téléchargées, donc rapide et reproductible
sur n'importe quelle machine (ENF2, ENF3).
"""

from src.controles import (
    AnomalieBloquante,
    Volumetrie,
    compter_cles_orphelines,
    compter_doublons,
    compter_hors_bornes,
    compter_incoherence_nb_sinistres,
    compter_lignes_dupliquees,
    compter_valeurs_egales,
    verifier_niveau,
)


def test_compter_doublons_detecte_une_cle_repetee(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["IDpol"])
    assert compter_doublons(df, "IDpol") == 1  # IDpol=1 apparaît 2 fois -> 1 clé en doublon


def test_compter_doublons_sans_doublon(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["IDpol"])
    assert compter_doublons(df, "IDpol") == 0


def test_compter_lignes_dupliquees(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["IDpol", "valeur"])
    assert compter_lignes_dupliquees(df) == 1


def test_compter_cles_orphelines(spark):
    contrats = spark.createDataFrame([(1,), (2,)], ["IDpol"])
    sinistres = spark.createDataFrame([(1,), (3,)], ["IDpol"])
    # IDpol=3 existe dans sinistres mais pas dans contrats -> 1 clé orpheline
    assert compter_cles_orphelines(sinistres, contrats, "IDpol") == 1


def test_compter_hors_bornes_deux_bornes(spark):
    df = spark.createDataFrame([(0.5,), (1.5,), (-0.1,)], ["Exposure"])
    assert compter_hors_bornes(df, "Exposure", borne_min=0, borne_max=1) == 2  # 1.5 et -0.1


def test_compter_hors_bornes_une_seule_borne(spark):
    df = spark.createDataFrame([(5,), (60,)], ["VehAge"])
    assert compter_hors_bornes(df, "VehAge", borne_max=50) == 1


def test_compter_incoherence_nb_sinistres_coherent(spark):
    contrats = spark.createDataFrame([(1, 1), (2, 0), (3, 2)], ["IDpol", "ClaimNb"])
    sinistres = spark.createDataFrame([(1,), (3,), (3,)], ["IDpol"])
    assert compter_incoherence_nb_sinistres(contrats, sinistres, "IDpol", "ClaimNb") == 0


def test_compter_incoherence_nb_sinistres_detecte_ecart(spark):
    contrats = spark.createDataFrame([(1, 0), (2, 0)], ["IDpol", "ClaimNb"])
    # sinistre pour le contrat 2 alors que son ClaimNb vaut 0 -> incohérence
    # (le contrat 1 reste cohérent : ClaimNb=0 et aucune ligne sinistre)
    sinistres = spark.createDataFrame([(2,)], ["IDpol"])
    assert compter_incoherence_nb_sinistres(contrats, sinistres, "IDpol", "ClaimNb") == 1


def test_compter_valeurs_egales(spark):
    df = spark.createDataFrame([(0.0,), (120.5,), (0.0,), (-50.0,)], ["ClaimAmount"])
    # un montant négatif (-50) n'est pas compté : seul le montant exactement nul l'est
    assert compter_valeurs_egales(df, "ClaimAmount", 0) == 2


def test_verifier_niveau_bloquant_leve_une_exception_si_anomalie(spark):
    try:
        verifier_niveau("doublons_contrats", 1, "bloquant")
        assert False, "une AnomalieBloquante aurait du etre levee"
    except AnomalieBloquante:
        pass


def test_verifier_niveau_bloquant_ne_leve_rien_si_aucune_anomalie(spark):
    verifier_niveau("doublons_contrats", 0, "bloquant")  # ne doit rien lever


def test_verifier_niveau_alerte_ne_leve_jamais(spark):
    verifier_niveau("exposition_hors_bornes", 1224, "alerte")  # ne doit rien lever


def test_volumetrie_enregistre_nb_lignes_et_sommes(spark):
    df1 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["IDpol", "ClaimAmount"])
    df2 = spark.createDataFrame([(1, 10.0)], ["IDpol", "ClaimAmount"])  # 1 ligne isolee a part

    volumetrie = Volumetrie()
    volumetrie.enregistrer("sinistres_bruts", df1, colonnes_sommees=("ClaimAmount",))
    volumetrie.enregistrer("sinistres_orphelins", df2, colonnes_sommees=("ClaimAmount",))

    tableau = volumetrie.tableau()
    assert tableau[0] == {"etape": "sinistres_bruts", "nb_lignes": 2, "somme_ClaimAmount": 30.0}
    assert tableau[1] == {"etape": "sinistres_orphelins", "nb_lignes": 1, "somme_ClaimAmount": 10.0}
