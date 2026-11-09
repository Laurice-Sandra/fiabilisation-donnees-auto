"""Construction du journal d'anomalies (EF3).

Contrairement à `src/controles.py` (qui ne fait que COMPTER les anomalies),
les fonctions "lister_*" ci-dessous renvoient le DÉTAIL de chaque anomalie
(un identifiant IDpol + la valeur observée) : c'est ce détail, pas un simple
total, que le cahier des charges demande dans le journal ("contrôle,
identifiant, valeur observée, niveau, date d'exécution").
"""

from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

COLONNES_JOURNAL = ["controle", "identifiant", "valeur_observee", "niveau", "date_execution"]


def lister_doublons(df: DataFrame, cle: str) -> pd.DataFrame:
    """Une ligne par valeur de `cle` en doublon, avec son nombre d'occurrences."""
    doublons = df.groupBy(cle).count().filter(F.col("count") > 1)
    pdf = doublons.toPandas().rename(columns={cle: "identifiant", "count": "valeur_observee"})
    return pdf[["identifiant", "valeur_observee"]]


def lister_lignes_dupliquees(df: DataFrame, cle: str, colonnes_dupliquees: list) -> pd.DataFrame:
    """Une ligne par groupe de lignes strictement identiques sur
    `colonnes_dupliquees` (ex: IDpol + ClaimAmount), en trop (count > 1).
    """
    groupes = df.groupBy(*colonnes_dupliquees).count().filter(F.col("count") > 1)
    pdf = groupes.select(cle, "count").toPandas().rename(
        columns={cle: "identifiant", "count": "valeur_observee"}
    )
    return pdf[["identifiant", "valeur_observee"]]


def lister_cles_orphelines(df_source: DataFrame, df_reference: DataFrame, cle: str, colonne_valeur: str) -> pd.DataFrame:
    """Une ligne par ligne de df_source dont `cle` n'existe pas dans df_reference."""
    cles_valides = df_reference.select(cle).distinct()
    orphelins = df_source.join(cles_valides, on=cle, how="left_anti")
    pdf = orphelins.select(cle, colonne_valeur).toPandas().rename(
        columns={cle: "identifiant", colonne_valeur: "valeur_observee"}
    )
    return pdf[["identifiant", "valeur_observee"]]


def lister_hors_bornes(df: DataFrame, cle: str, colonne: str, borne_min: float | None = None, borne_max: float | None = None) -> pd.DataFrame:
    """Une ligne par ligne dont `colonne` est hors de [borne_min, borne_max]."""
    condition = F.lit(False)
    if borne_min is not None:
        condition = condition | (F.col(colonne) < borne_min)
    if borne_max is not None:
        condition = condition | (F.col(colonne) > borne_max)
    pdf = df.filter(condition).select(cle, colonne).toPandas().rename(
        columns={cle: "identifiant", colonne: "valeur_observee"}
    )
    return pdf[["identifiant", "valeur_observee"]]


def lister_incoherence_nb_sinistres(df_jointe: DataFrame, cle: str, colonne_claimnb: str = "ClaimNb") -> pd.DataFrame:
    """Une ligne par contrat où ClaimNb != nb_sinistres_reel, avec les deux
    valeurs dans `valeur_observee` (ex: "ClaimNb=2;nb_sinistres_reel=0").
    """
    ecart = df_jointe.filter(F.col(colonne_claimnb) != F.col("nb_sinistres_reel"))
    pdf = ecart.select(
        F.col(cle).alias("identifiant"),
        F.format_string(
            "ClaimNb=%d;nb_sinistres_reel=%d", F.col(colonne_claimnb), F.col("nb_sinistres_reel")
        ).alias("valeur_observee"),
    ).toPandas()
    return pdf


def construire_journal(lignes_par_controle: dict, niveaux: dict) -> pd.DataFrame:
    """Assemble les tableaux `lister_*` en un seul journal, une ligne par
    anomalie individuelle, avec les colonnes attendues par EF3.
    """
    date_execution = datetime.now(timezone.utc).isoformat(timespec="seconds")
    morceaux = []
    for nom_controle, pdf in lignes_par_controle.items():
        if pdf.empty:
            continue
        copie = pdf.copy()
        copie.insert(0, "controle", nom_controle)
        copie["niveau"] = niveaux[nom_controle]
        copie["date_execution"] = date_execution
        morceaux.append(copie[COLONNES_JOURNAL])
    if not morceaux:
        return pd.DataFrame(columns=COLONNES_JOURNAL)
    return pd.concat(morceaux, ignore_index=True)
