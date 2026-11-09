"""Réconciliation indépendante : recalcul des totaux en pandas, en dehors de
Spark, pour vérifier qu'aucun contrat/sinistre/euro n'a été perdu ou modifié
pendant le traitement Spark (EF6 / ENF6).
"""

from pathlib import Path

import pandas as pd

from src.telechargement import RACINE_PROJET, charger_parametres


def recalculer_totaux_pandas(parametres: dict | None = None, racine: Path = RACINE_PROJET) -> dict:
    """Relit les CSV bruts directement en pandas (sans passer par Spark ni
    par le reste du pipeline) et recalcule les totaux de référence.

    `racine` est paramétrable (défaut : racine réelle du projet) pour
    pouvoir tester cette fonction sur de petits CSV temporaires, sans
    dépendre des vraies données téléchargées.
    """
    parametres = parametres or charger_parametres()
    chemin_contrats = racine / parametres["sources"]["contrats"]["fichier"]
    chemin_sinistres = racine / parametres["sources"]["sinistres"]["fichier"]

    contrats = pd.read_csv(chemin_contrats)
    sinistres = pd.read_csv(chemin_sinistres)

    return {
        "nb_contrats": len(contrats),
        "exposition_totale": float(contrats["Exposure"].sum()),
        "nb_sinistres": len(sinistres),
        "charge_totale": float(sinistres["ClaimAmount"].sum()),
    }


def comparer_totaux(totaux_pipeline: dict, totaux_pandas: dict, tolerance: float) -> pd.DataFrame:
    """Compare chaque total du pipeline (Spark) à son recalcul indépendant
    (pandas), avec un écart relatif et un statut "ok" (dans la tolérance du
    YAML) ou pas. Renvoie un tableau, pas juste un booléen global : un écart
    isolé doit rester visible même si le reste concorde.
    """
    lignes = []
    for cle in totaux_pipeline:
        valeur_pipeline = totaux_pipeline[cle]
        valeur_pandas = totaux_pandas[cle]
        ecart_relatif = (
            abs(valeur_pipeline - valeur_pandas) / abs(valeur_pandas) if valeur_pandas else 0.0
        )
        lignes.append({
            "indicateur": cle,
            "pipeline": valeur_pipeline,
            "pandas": valeur_pandas,
            "ecart_relatif": ecart_relatif,
            "ok": ecart_relatif <= tolerance,
        })
    return pd.DataFrame(lignes)
