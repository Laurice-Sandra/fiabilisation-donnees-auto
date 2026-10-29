"""Tests de base sur le chargement de la configuration du téléchargement.

Ces tests ne font aucun appel réseau : ils vérifient seulement que
config/parametres.yaml est lisible et contient les clés attendues.
"""

from src.telechargement import RACINE_PROJET, charger_parametres


def test_racine_projet_contient_bien_la_config():
    assert (RACINE_PROJET / "config" / "parametres.yaml").is_file()


def test_parametres_contiennent_les_deux_sources():
    parametres = charger_parametres()
    assert set(parametres["sources"]) == {"contrats", "sinistres"}
    for source in parametres["sources"].values():
        assert "openml_id" in source
        assert "fichier" in source
