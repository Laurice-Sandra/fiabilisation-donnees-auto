"""Chargement des CSV contrats/sinistres en DataFrames Spark, avec un schéma
explicite (pas `inferSchema=True` : déterministe, pas de second passage sur
le fichier, documente le contrat de données) et le nettoyage IDpol.

Ce module centralise une logique auparavant dupliquée dans le notebook et
dans `src/rapport.py` (ENF2 : une seule source de vérité, plus simple à
maintenir et à tester).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.telechargement import RACINE_PROJET, charger_parametres, telecharger_sources

SCHEMA_CONTRATS = StructType(
    [
        StructField("IDpol", DoubleType(), nullable=False),
        StructField("ClaimNb", IntegerType(), nullable=False),
        StructField("Exposure", DoubleType(), nullable=False),
        StructField("Area", StringType(), nullable=True),
        StructField("VehPower", IntegerType(), nullable=False),
        StructField("VehAge", IntegerType(), nullable=False),
        StructField("DrivAge", IntegerType(), nullable=False),
        StructField("BonusMalus", IntegerType(), nullable=False),
        StructField("VehBrand", StringType(), nullable=True),
        StructField("VehGas", StringType(), nullable=True),
        StructField("Density", IntegerType(), nullable=False),
        StructField("Region", StringType(), nullable=True),
    ]
)

SCHEMA_SINISTRES = StructType(
    [
        StructField("IDpol", DoubleType(), nullable=False),
        StructField("ClaimAmount", DoubleType(), nullable=False),
    ]
)


def _nettoyer_idpol(df: DataFrame) -> DataFrame:
    """IDpol est la clé de jointure : un identifiant de police est une clé,
    pas une grandeur numérique, donc on le type en string. Le CSV contrats
    écrit "1.0" (artefact de l'export R d'origine) ; on repasse par un
    entier pour obtenir "1" plutôt que "1.0" avant de convertir en texte.
    """
    return df.withColumn("IDpol", F.col("IDpol").cast("long").cast("string"))


def charger_contrats_sinistres(spark: SparkSession, parametres: dict | None = None) -> tuple[DataFrame, DataFrame]:
    """Télécharge (si besoin) et charge les deux tables, schéma explicite +
    IDpol nettoyé. Point d'entrée unique réutilisé par le notebook, le
    rapport et le pipeline.
    """
    parametres = parametres or charger_parametres()
    telecharger_sources(parametres)

    chemin_contrats = RACINE_PROJET / parametres["sources"]["contrats"]["fichier"]
    chemin_sinistres = RACINE_PROJET / parametres["sources"]["sinistres"]["fichier"]

    df_contrats = spark.read.csv(str(chemin_contrats), header=True, schema=SCHEMA_CONTRATS)
    df_sinistres = spark.read.csv(str(chemin_sinistres), header=True, schema=SCHEMA_SINISTRES)

    return _nettoyer_idpol(df_contrats), _nettoyer_idpol(df_sinistres)
