"""Tests des fonctions de jointure de src/jointure.py.

Petits DataFrames Spark en mémoire, pas de dépendance aux vraies données
(ENF2, ENF3).
"""

from src.jointure import (
    agreger_sinistres,
    calculer_ecart_nb_sinistres,
    detailler_ecart_nb_sinistres,
    isoler_orphelins,
    joindre_contrats_sinistres,
)


def test_agreger_sinistres_additionne_montant_et_compte_les_lignes(spark):
    # IDpol=1 a 2 lignes de sinistres, dont 2 identiques (pas filtrées)
    sinistres = spark.createDataFrame(
        [(1, 100.0), (1, 100.0), (2, 50.0)], ["IDpol", "ClaimAmount"]
    )
    agrege = agreger_sinistres(sinistres, "IDpol").orderBy("IDpol").collect()
    assert agrege[0]["IDpol"] == 1
    assert agrege[0]["montant_total_sinistres"] == 200.0
    assert agrege[0]["nb_sinistres_reel"] == 2
    assert agrege[1]["montant_total_sinistres"] == 50.0
    assert agrege[1]["nb_sinistres_reel"] == 1


def test_isoler_orphelins(spark):
    contrats = spark.createDataFrame([(1,), (2,)], ["IDpol"])
    sinistres = spark.createDataFrame([(1, 10.0), (3, 20.0)], ["IDpol", "ClaimAmount"])
    orphelins = isoler_orphelins(sinistres, contrats, "IDpol")
    assert orphelins.count() == 1
    assert orphelins.collect()[0]["IDpol"] == 3


def test_joindre_contrats_sinistres_remplit_les_contrats_sans_sinistre(spark):
    contrats = spark.createDataFrame([(1, 1), (2, 0)], ["IDpol", "ClaimNb"])
    sinistres = spark.createDataFrame([(1, 100.0)], ["IDpol", "ClaimAmount"])
    jointe = joindre_contrats_sinistres(contrats, sinistres, "IDpol").orderBy("IDpol").collect()
    assert jointe[0]["nb_sinistres_reel"] == 1
    assert jointe[0]["montant_total_sinistres"] == 100.0
    # contrat 2 n'a aucun sinistre -> 0, pas de valeur nulle
    assert jointe[1]["nb_sinistres_reel"] == 0
    assert jointe[1]["montant_total_sinistres"] == 0.0


def test_joindre_contrats_sinistres_signale_la_coherence_nb_sinistres(spark):
    # contrat 1 : ClaimNb=1 = nb_sinistres_reel=1 -> coherent
    # contrat 2 : ClaimNb=2 != nb_sinistres_reel=0 (pas de sinistre trouve) -> pas coherent
    contrats = spark.createDataFrame([(1, 1), (2, 2)], ["IDpol", "ClaimNb"])
    sinistres = spark.createDataFrame([(1, 100.0)], ["IDpol", "ClaimAmount"])
    jointe = joindre_contrats_sinistres(contrats, sinistres, "IDpol").orderBy("IDpol").collect()
    assert jointe[0]["nb_sinistres_coherent"] is True
    assert jointe[1]["nb_sinistres_coherent"] is False


def test_joindre_contrats_sinistres_garde_tous_les_contrats(spark):
    # LEFT JOIN : le nombre de contrats ne doit jamais changer
    contrats = spark.createDataFrame([(1, 0), (2, 0), (3, 0)], ["IDpol", "ClaimNb"])
    sinistres = spark.createDataFrame([(1, 10.0)], ["IDpol", "ClaimAmount"])
    jointe = joindre_contrats_sinistres(contrats, sinistres, "IDpol")
    assert jointe.count() == contrats.count()


def test_calculer_ecart_nb_sinistres(spark):
    # contrat 1 : coherent (ClaimNb=1, nb_sinistres_reel=1)
    # contrat 2 : ClaimNb=2 mais nb_sinistres_reel=0 -> absence totale
    # contrat 3 : ClaimNb=1 mais nb_sinistres_reel=2 -> simple ecart
    df_jointe = spark.createDataFrame(
        [
            (1, 1, 1, 100.0),
            (2, 2, 0, 0.0),
            (3, 1, 2, 50.0),
        ],
        ["IDpol", "ClaimNb", "nb_sinistres_reel", "montant_total_sinistres"],
    )
    resultat = calculer_ecart_nb_sinistres(df_jointe)
    assert resultat["nb_contrats_ecart"] == 2
    assert resultat["montant_concerne"] == 50.0
    assert resultat["nb_contrats_claimnb_sans_sinistre"] == 1


def test_detailler_ecart_nb_sinistres(spark):
    # memes 3 contrats que le test precedent, plus un 4eme contrat avec le
    # meme ecart que le contrat 3 (+1), pour verifier le groupBy et la moyenne
    df_jointe = spark.createDataFrame(
        [
            (1, 1, 1, 100.0),   # coherent, absent du detail
            (2, 2, 0, 0.0),     # ecart = 0 - 2 = -2
            (3, 1, 2, 50.0),    # ecart = 2 - 1 = +1
            (4, 1, 2, 150.0),   # ecart = 2 - 1 = +1
        ],
        ["IDpol", "ClaimNb", "nb_sinistres_reel", "montant_total_sinistres"],
    )
    detail = detailler_ecart_nb_sinistres(df_jointe).orderBy("ecart_nb_sinistres").collect()
    assert len(detail) == 2  # deux valeurs d'ecart distinctes : -2 et +1
    assert detail[0]["ecart_nb_sinistres"] == -2
    assert detail[0]["nb_contrats"] == 1
    assert detail[0]["montant_moyen"] == 0.0
    assert detail[1]["ecart_nb_sinistres"] == 1
    assert detail[1]["nb_contrats"] == 2
    assert detail[1]["montant_moyen"] == 100.0  # (50 + 150) / 2
