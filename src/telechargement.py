"""Téléchargement des sources OpenML décrites dans config/parametres.yaml.

Ce module est la seule brique autorisée à parler à OpenML : le notebook et
le futur pipeline appellent `telecharger_sources()` au lieu de refaire ce
code. La racine du projet est calculée à partir de l'emplacement de ce
fichier (`__file__`), jamais du dossier courant du kernel : le résultat est
donc identique quel que soit l'endroit d'où le code est lancé.
"""

import urllib.request
from pathlib import Path

import pandas as pd
import yaml
from sklearn.datasets import fetch_openml

RACINE_PROJET = Path(__file__).resolve().parent.parent
CHEMIN_CONFIG = RACINE_PROJET / "config" / "parametres.yaml"


def charger_parametres(chemin_config: Path = CHEMIN_CONFIG) -> dict:
    """Lit config/parametres.yaml et renvoie son contenu sous forme de dict."""
    with chemin_config.open(encoding="utf-8") as fichier:
        return yaml.safe_load(fichier)


def telecharger_sources(parametres: dict | None = None) -> dict[str, pd.DataFrame]:
    """Télécharge chaque source déclarée dans parametres["sources"] et
    l'enregistre au chemin indiqué dans le YAML.

    Renvoie un dict {nom_source: DataFrame} pour une utilisation immédiate
    (notebook, pipeline) sans relire les fichiers CSV.
    """
    parametres = parametres or charger_parametres()
    tables: dict[str, pd.DataFrame] = {}
    for nom, source in parametres["sources"].items():
        chemin = RACINE_PROJET / source["fichier"]
        chemin.parent.mkdir(parents=True, exist_ok=True)
        table = fetch_openml(data_id=source["openml_id"], as_frame=True).frame
        table.to_csv(chemin, index=False)
        tables[nom] = table
    return tables


def telecharger_reference_geographique(parametres: dict | None = None, nom: str = "regions_geojson") -> Path:
    """Télécharge un fichier de référence géographique (pas OpenML, pas de
    cache intégré comme fetch_openml) déclaré dans parametres["references"].

    Idempotent "à la main" : si le fichier existe déjà sur disque, on ne
    retélécharge pas (pas de dépendance réseau à chaque relance, cf. ENF2
    reproductibilité < 10 min).
    """
    parametres = parametres or charger_parametres()
    reference = parametres["references"][nom]
    chemin = RACINE_PROJET / reference["fichier"]
    if not chemin.exists():
        chemin.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(reference["url"], chemin)
    return chemin
