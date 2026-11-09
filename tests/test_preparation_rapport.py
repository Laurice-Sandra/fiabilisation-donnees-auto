"""Tests des fonctions de préparation du rapport (src/preparation_rapport.py).

Petits DataFrames Spark en mémoire, pas de dépendance réseau ni HTML (ENF3).
"""

from src.preparation_rapport import (
    agreger_par_region,
    agreger_par_segment,
    calculer_kpis_globaux,
)


def _df_minimal(spark):
    return spark.createDataFrame(
        [
            (1, "R11", 1.0, 1, 1, 100.0),
            (2, "R11", 0.5, 0, 0, 0.0),
            (3, "R24", 1.0, 2, 2, 300.0),
        ],
        ["IDpol", "Region", "Exposure", "ClaimNb", "nb_sinistres_reel", "montant_total_sinistres"],
    )


def test_calculer_kpis_globaux(spark):
    df = _df_minimal(spark)
    kpis = calculer_kpis_globaux(df)
    assert kpis["nb_contrats"] == 3
    assert kpis["exposition_totale"] == 2.5
    assert kpis["claimnb_total"] == 3
    assert kpis["nb_sinistres_reel_total"] == 3
    assert kpis["charge_totale"] == 400.0
    assert kpis["frequence_globale"] == 3 / 2.5
    assert kpis["cout_moyen_global"] == 400.0 / 3
    assert kpis["prime_pure_globale"] == kpis["frequence_globale"] * kpis["cout_moyen_global"]


def test_agreger_par_segment_une_colonne(spark):
    df = _df_minimal(spark)
    agrege = agreger_par_segment(df, "Region").set_index("Region")
    assert agrege.loc["R11", "nb_contrats"] == 2
    assert agrege.loc["R11", "exposition_totale"] == 1.5
    assert agrege.loc["R11", "nb_sinistres_reel"] == 1
    assert agrege.loc["R24", "nb_contrats"] == 1
    assert agrege.loc["R24", "cout_moyen"] == 150.0


def test_agreger_par_segment_cout_moyen_nan_si_aucun_sinistre(spark):
    df = _df_minimal(spark).filter("IDpol = 2")  # seul contrat sans sinistre
    agrege = agreger_par_segment(df, "Region")
    assert agrege["cout_moyen"].isna().all()


def test_agreger_par_region_ajoute_le_code_sans_prefixe(spark):
    df = _df_minimal(spark)
    agrege = agreger_par_region(df).set_index("Region")
    assert agrege.loc["R11", "code"] == "11"
    assert agrege.loc["R24", "code"] == "24"
