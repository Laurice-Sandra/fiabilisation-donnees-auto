"""Tests de src/reconciliation.py (petits CSV temporaires, pas de réseau)."""

import pandas as pd

from src.reconciliation import comparer_totaux, recalculer_totaux_pandas


def test_recalculer_totaux_pandas(tmp_path):
    (tmp_path / "data").mkdir()
    chemin_contrats = tmp_path / "data" / "contrats.csv"
    chemin_sinistres = tmp_path / "data" / "sinistres.csv"
    pd.DataFrame({"IDpol": [1, 2, 3], "Exposure": [0.5, 1.0, 0.25]}).to_csv(chemin_contrats, index=False)
    pd.DataFrame({"IDpol": [1, 1], "ClaimAmount": [100.0, 50.0]}).to_csv(chemin_sinistres, index=False)

    parametres = {
        "sources": {
            "contrats": {"fichier": "data/contrats.csv"},
            "sinistres": {"fichier": "data/sinistres.csv"},
        }
    }
    totaux = recalculer_totaux_pandas(parametres, racine=tmp_path)
    assert totaux["nb_contrats"] == 3
    assert totaux["exposition_totale"] == 1.75
    assert totaux["nb_sinistres"] == 2
    assert totaux["charge_totale"] == 150.0


def test_comparer_totaux_dans_la_tolerance():
    resultat = comparer_totaux(
        {"nb_contrats": 100, "charge_totale": 1000.0},
        {"nb_contrats": 100, "charge_totale": 999.0},
        tolerance=0.01,
    ).set_index("indicateur")
    assert resultat.loc["nb_contrats", "ok"]
    assert resultat.loc["charge_totale", "ok"]  # ecart de 0.1% < 1%


def test_comparer_totaux_hors_tolerance():
    resultat = comparer_totaux(
        {"charge_totale": 1200.0},
        {"charge_totale": 1000.0},
        tolerance=0.01,
    ).set_index("indicateur")
    assert not resultat.loc["charge_totale", "ok"]
    assert resultat.loc["charge_totale", "ecart_relatif"] == 0.2
