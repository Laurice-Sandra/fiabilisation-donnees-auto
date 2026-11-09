"""Tests de src/chargement.py.

Pas de test réseau sur `charger_contrats_sinistres` (elle déclenche un
téléchargement OpenML) : on teste séparément le nettoyage IDpol (logique
pure, ENF3) et le contenu des schémas explicites.
"""

from src.chargement import SCHEMA_CONTRATS, SCHEMA_SINISTRES, _nettoyer_idpol


def test_schema_contrats_a_les_bonnes_colonnes():
    assert SCHEMA_CONTRATS.names == [
        "IDpol", "ClaimNb", "Exposure", "Area", "VehPower", "VehAge",
        "DrivAge", "BonusMalus", "VehBrand", "VehGas", "Density", "Region",
    ]


def test_schema_sinistres_a_les_bonnes_colonnes():
    assert SCHEMA_SINISTRES.names == ["IDpol", "ClaimAmount"]


def test_nettoyer_idpol_enleve_le_point_zero(spark):
    df = spark.createDataFrame([(1.0,), (3.0,), (1552.0,)], ["IDpol"])
    resultat = _nettoyer_idpol(df).collect()
    valeurs = {row["IDpol"] for row in resultat}
    assert valeurs == {"1", "3", "1552"}
