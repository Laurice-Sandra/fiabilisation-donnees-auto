"""Pipeline complet : une seule commande (EF1) qui enchaîne téléchargement,
chargement, contrôles (EF2), jointure, écriture de la base fiabilisée,
journal d'anomalies (EF3), réconciliation indépendante (EF6) et reporting
HTML (EF5).

Lancement : `python -m src.pipeline` depuis la racine du projet.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.chargement import charger_contrats_sinistres
from src.controles import AnomalieBloquante, Volumetrie, verifier_niveau
from src.jointure import isoler_orphelins, joindre_contrats_sinistres
from src.journal import (
    construire_journal,
    lister_cles_orphelines,
    lister_doublons,
    lister_hors_bornes,
    lister_incoherence_nb_sinistres,
    lister_lignes_dupliquees,
)
from src.rapport import generer_rapport
from src.reconciliation import comparer_totaux, recalculer_totaux_pandas
from src.telechargement import RACINE_PROJET, charger_parametres


def _creer_spark_session() -> SparkSession:
    import os

    os.environ["JAVA_HOME"] = os.path.join(sys.prefix, "Library")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    return (
        SparkSession.builder.master("local[*]")
        .appName("pipeline_fiabilisation")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def executer_pipeline(parametres: dict | None = None) -> dict:
    """Exécute le pipeline complet et renvoie un résumé (volumétrie,
    réconciliation, chemins des livrables écrits).
    """
    parametres = parametres or charger_parametres()
    cle = parametres["jointure"]["cle"]
    niveaux = parametres["controles"]["niveaux"]

    spark = _creer_spark_session()
    try:
        df_contrats, df_sinistres = charger_contrats_sinistres(spark, parametres)

        volumetrie = Volumetrie()
        volumetrie.enregistrer("contrats_bruts", df_contrats, colonnes_sommees=("Exposure",))
        volumetrie.enregistrer("sinistres_bruts", df_sinistres, colonnes_sommees=("ClaimAmount",))

        # EF2 : controle bloquant AVANT la jointure (doublons de contrat).
        # Si bloquant et anomalies trouvees, AnomalieBloquante remonte et
        # arrete tout le traitement (comportement voulu : "bloquant" = arret).
        nb_doublons_contrats = lister_doublons(df_contrats, cle).shape[0]
        verifier_niveau("doublons_contrats", nb_doublons_contrats, niveaux["doublons_contrats"])

        df_orphelins = isoler_orphelins(df_sinistres, df_contrats, cle)
        volumetrie.enregistrer("sinistres_orphelins", df_orphelins, colonnes_sommees=("ClaimAmount",))

        df_base = joindre_contrats_sinistres(df_contrats, df_sinistres, cle)
        volumetrie.enregistrer("base_fiabilisee", df_base, colonnes_sommees=("montant_total_sinistres",))

        # EF3 : journal d'anomalies, detail ligne par ligne (pas un total)
        exposition = parametres["controles"]
        lignes_par_controle = {
            "doublons_contrats": lister_doublons(df_contrats, cle),
            "doublons_sinistres": lister_lignes_dupliquees(df_sinistres, cle, [cle, "ClaimAmount"]),
            "cles_orphelines": lister_cles_orphelines(df_sinistres, df_contrats, cle, "ClaimAmount"),
            "coherence_nb_sinistres": lister_incoherence_nb_sinistres(df_base, cle),
            "exposition_hors_bornes": lister_hors_bornes(
                df_base, cle, "Exposure", exposition["exposition_min"], exposition["exposition_max"]
            ),
        }
        journal = construire_journal(lignes_par_controle, niveaux)
        chemin_journal = RACINE_PROJET / parametres["sorties"]["journal_anomalies"]
        chemin_journal.parent.mkdir(parents=True, exist_ok=True)
        journal.to_csv(chemin_journal, index=False)

        # Ecriture de la base fiabilisee et des orphelins (livrables EF).
        # Via pandas/pyarrow plutot que df.write.parquet() de Spark : sur
        # Windows, l'ecriture Spark native necessite winutils.exe/HADOOP_HOME
        # (absent par defaut, cf. contrainte "Windows, Anaconda" du cahier
        # des charges). Tout le calcul reste en PySpark (ENF6) ; seule cette
        # derniere etape (ecriture disque) bascule en pandas, un seul
        # fichier .parquet classique plutot qu'un dossier partitionne Spark.
        chemin_base = RACINE_PROJET / parametres["sorties"]["base_fiabilisee"]
        chemin_base.parent.mkdir(parents=True, exist_ok=True)
        df_base.toPandas().to_parquet(chemin_base, index=False)

        chemin_orphelins = RACINE_PROJET / parametres["sorties"]["sinistres_orphelins"]
        df_orphelins.toPandas().to_parquet(chemin_orphelins, index=False)

        # EF6 : reconciliation independante (pandas, en dehors de Spark)
        totaux_pipeline = (
            df_contrats.agg(
                F.count("*").alias("nb_contrats"),
                F.sum("Exposure").alias("exposition_totale"),
            ).toPandas().iloc[0].to_dict()
            | df_sinistres.agg(
                F.count("*").alias("nb_sinistres"),
                F.sum("ClaimAmount").alias("charge_totale"),
            ).toPandas().iloc[0].to_dict()
        )
        totaux_pandas = recalculer_totaux_pandas(parametres)
        reconciliation = comparer_totaux(
            totaux_pipeline, totaux_pandas, parametres["controles"]["tolerance_reconciliation"]
        )

        # EF5 : reporting HTML
        chemin_rapport = generer_rapport(spark, parametres)

        return {
            "volumetrie": volumetrie.tableau(),
            "journal_anomalies": str(chemin_journal),
            "nb_anomalies_journalisees": len(journal),
            "base_fiabilisee": str(chemin_base),
            "sinistres_orphelins": str(chemin_orphelins),
            "reconciliation": reconciliation,
            "reconciliation_ok": bool(reconciliation["ok"].all()),
            "rapport_controle": chemin_rapport,
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    resume = executer_pipeline()
    print("=== Pipeline termine ===")
    print("Volumetrie :", resume["volumetrie"])
    print("Journal d'anomalies :", resume["journal_anomalies"], f"({resume['nb_anomalies_journalisees']} lignes)")
    print("Base fiabilisee :", resume["base_fiabilisee"])
    print("Sinistres orphelins :", resume["sinistres_orphelins"])
    print("Reconciliation (pandas vs pipeline) :")
    print(resume["reconciliation"].to_string(index=False))
    print("Reconciliation OK :", resume["reconciliation_ok"])
    print("Rapport de controle :", resume["rapport_controle"])
    if not resume["reconciliation_ok"]:
        sys.exit(1)
