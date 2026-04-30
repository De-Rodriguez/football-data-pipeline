# =============================================================================
# transformation_databricks.py — Architecture Medallion (Bronze / Silver / Gold)
# =============================================================================
# Auteur  : Étudiant 3iL (Portfolio Alternance)
# Runtime : Databricks (Apache Spark 3.x + Delta Lake)
#
# Comment importer dans Databricks :
#   1. Workspace → Import → "File" → sélectionner ce .py
#   OU coller le contenu dans un notebook Databricks cellule par cellule.
#
# Pré-requis Databricks :
#   - Cluster avec Delta Lake activé (inclus par défaut sur DBR 8+).
#   - Fichier raw_real_madrid_stats.json uploadé dans DBFS :
#       dbutils.fs.cp("file:/tmp/raw_real_madrid_stats.json",
#                     "dbfs:/data/football/raw/raw_real_madrid_stats.json")
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ---------------------------------------------------------------------------
# 0. Initialisation Spark (déjà disponible dans Databricks via `spark`)
# ---------------------------------------------------------------------------
# En local, décommenter la ligne suivante pour les tests hors Databricks :
# spark = SparkSession.builder.appName("FootballMedallion").getOrCreate()

# Chemins DBFS (système de fichiers distribué Databricks)
RAW_JSON_PATH   = "dbfs:/data/football/raw/raw_real_madrid_stats.json"
BRONZE_PATH     = "dbfs:/data/football/bronze/real_madrid"
SILVER_PATH     = "dbfs:/data/football/silver/real_madrid_players"
GOLD_PATH       = "dbfs:/data/football/gold/real_madrid_performance"


# =============================================================================
# COUCHE BRONZE — Ingestion brute (source of truth)
# =============================================================================
# Objectif : Lire le JSON brut et le stocker tel quel en Delta.
#            Aucune transformation — on préserve la donnée originale.
# =============================================================================

def create_bronze_layer(raw_path: str, bronze_path: str):
    """
    Lit le JSON brut et le persiste en table Delta Bronze.
    La structure imbriquée est conservée intacte.

    Args:
        raw_path   : Chemin DBFS vers le fichier JSON brut.
        bronze_path: Chemin DBFS de destination pour la table Delta Bronze.
    """
    print("=" * 60)
    print("🥉 BRONZE — Chargement du JSON brut")
    print("=" * 60)

    # Lecture du JSON brut — multiLine=True car le JSON est multi-lignes
    df_bronze = (
        spark.read
        .option("multiLine", "true")
        .json(raw_path)
    )

    print(f"  Schéma brut détecté :")
    df_bronze.printSchema()
    print(f"  Nombre de lignes brutes : {df_bronze.count()}")

    # Écriture en Delta — mode overwrite pour idempotence
    (
        df_bronze
        .write
        .format("delta")
        .mode("overwrite")
        .save(bronze_path)
    )

    print(f"  ✅ Bronze sauvegardé → {bronze_path}\n")
    return df_bronze


# =============================================================================
# COUCHE SILVER — Nettoyage et aplatissement (flattening)
# =============================================================================
# Objectif : Transformer la structure imbriquée en table plate et typée.
#
# Structure source (profondeur 4) :
#   response
#     └─ list
#          └─ squad          (Array<Struct>)  ← explode #1
#               └─ members   (Array<Struct>)  ← explode #2
# =============================================================================

def create_silver_layer(bronze_path: str, silver_path: str):
    """
    Aplatit (flatten) la structure JSON imbriquée en table plate.
    Applique le nettoyage des types et filtre les enregistrements invalides.

    Args:
        bronze_path: Chemin DBFS de la table Delta Bronze.
        silver_path: Chemin DBFS de destination pour la table Delta Silver.
    """
    print("=" * 60)
    print("🥈 SILVER — Aplatissement et nettoyage des données")
    print("=" * 60)

    # Lecture de la couche Bronze
    df_bronze = spark.read.format("delta").load(bronze_path)

    # -------------------------------------------------------------------------
    # Étape 1 : Extraire le tableau `squad` depuis la structure imbriquée
    #           response.list.squad → Array de structs (un par type de poste)
    # -------------------------------------------------------------------------
    df_squads = df_bronze.select(
        F.explode("response.list.squad").alias("squad_group")
    )

    # -------------------------------------------------------------------------
    # Étape 2 : Aplatir `members` depuis chaque groupe de squad
    #           squad_group.members → Array de structs (un par joueur)
    # -------------------------------------------------------------------------
    df_players = df_squads.select(
        F.col("squad_group.type").alias("position_group"),
        F.explode("squad_group.members").alias("player")
    )

    # -------------------------------------------------------------------------
    # Étape 3 : Extraire les champs de chaque joueur + nettoyage des types
    # -------------------------------------------------------------------------
    df_silver = df_players.select(
        # Identifiant unique du joueur
        F.col("player.id").cast(IntegerType()).alias("player_id"),

        # Informations générales
        F.col("player.name").cast(StringType()).alias("name"),
        F.col("player.age").cast(IntegerType()).alias("age"),
        F.col("player.nationality").cast(StringType()).alias("nationality"),
        F.col("player.position").cast(StringType()).alias("position"),
        F.col("position_group"),

        # Statistiques — typage explicite pour éviter les erreurs en aval
        F.col("player.stats.goals").cast(IntegerType()).alias("goals"),
        F.col("player.stats.assists").cast(IntegerType()).alias("assists"),
        F.col("player.stats.appearances").cast(IntegerType()).alias("appearances"),

        # Valeur de transfert en Double (peut être null → coalesce)
        F.coalesce(
            F.col("player.stats.transfer_value").cast(DoubleType()),
            F.lit(0.0)
        ).alias("transfer_value_eur"),

        # Métadonnée de traitement
        F.current_timestamp().alias("processed_at"),
    )

    # -------------------------------------------------------------------------
    # Étape 4 : Filtres qualité — suppression des enregistrements invalides
    # -------------------------------------------------------------------------
    df_silver = df_silver.filter(
        F.col("player_id").isNotNull() &
        F.col("name").isNotNull() &
        (F.col("goals") >= 0) &
        (F.col("appearances") >= 0)
    )

    print(f"  Schéma Silver :")
    df_silver.printSchema()
    print(f"  Nombre de joueurs après nettoyage : {df_silver.count()}")
    df_silver.show(5, truncate=False)

    # Écriture en Delta Silver
    (
        df_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    print(f"  ✅ Silver sauvegardé → {silver_path}\n")
    return df_silver


# =============================================================================
# COUCHE GOLD — Agrégats métier (performance table)
# =============================================================================
# Objectif : Produire une table de performance prête pour la BI / ML.
#            Calcul : score_performance = buts + passes décisives
# =============================================================================

def create_gold_layer(silver_path: str, gold_path: str):
    """
    Construit la table Gold de performance à partir de la couche Silver.
    Calcule un score composite et classe les joueurs.

    Args:
        silver_path: Chemin DBFS de la table Delta Silver.
        gold_path  : Chemin DBFS de destination pour la table Delta Gold.
    """
    print("=" * 60)
    print("🥇 GOLD — Table de performance agrégée")
    print("=" * 60)

    df_silver = spark.read.format("delta").load(silver_path)

    # -------------------------------------------------------------------------
    # Calcul du score de performance : Buts + Passes décisives
    # -------------------------------------------------------------------------
    df_gold = df_silver.select(
        "player_id",
        "name",
        "age",
        "nationality",
        "position",
        "position_group",
        "goals",
        "assists",
        "appearances",
        "transfer_value_eur",

        # Score de performance composite
        (F.col("goals") + F.col("assists")).alias("performance_score"),

        # Buts par match (arrondi à 2 décimales, évite division par zéro)
        F.round(
            F.col("goals") / F.when(F.col("appearances") == 0, 1)
                              .otherwise(F.col("appearances")),
            2
        ).alias("goals_per_game"),

        # Passes par match
        F.round(
            F.col("assists") / F.when(F.col("appearances") == 0, 1)
                               .otherwise(F.col("appearances")),
            2
        ).alias("assists_per_game"),

        # Rang de performance dans le squad (1 = meilleur)
        F.dense_rank()
         .over(
             __import__("pyspark.sql.window", fromlist=["Window"])
             .Window.orderBy(F.desc("performance_score"))
         )
         .alias("performance_rank"),

        # Catégorie de valeur marchande
        F.when(F.col("transfer_value_eur") >= 100_000_000, "Classe Mondiale")
         .when(F.col("transfer_value_eur") >= 50_000_000,  "Haut Niveau")
         .when(F.col("transfer_value_eur") >= 10_000_000,  "Standard")
         .otherwise("Rotation")
         .alias("market_tier"),

        "processed_at",
    )

    # -------------------------------------------------------------------------
    # Affichage du Top 10 — meilleurs performeurs
    # -------------------------------------------------------------------------
    print("  🏆 Top 10 des joueurs par score de performance :")
    (
        df_gold
        .orderBy(F.desc("performance_score"))
        .select("name", "position_group", "goals", "assists", "performance_score", "market_tier")
        .show(10, truncate=False)
    )

    # -------------------------------------------------------------------------
    # Agrégat par groupe de position (résumé équipe)
    # -------------------------------------------------------------------------
    print("  📊 Résumé par groupe de position :")
    (
        df_gold
        .groupBy("position_group")
        .agg(
            F.count("player_id").alias("nb_joueurs"),
            F.sum("goals").alias("total_buts"),
            F.sum("assists").alias("total_passes"),
            F.round(F.avg("transfer_value_eur"), 0).alias("valeur_moy_eur"),
        )
        .orderBy("position_group")
        .show(truncate=False)
    )

    # Écriture en Delta Gold — partitionné par groupe de position
    (
        df_gold
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("position_group")
        .save(gold_path)
    )

    print(f"  ✅ Gold sauvegardé → {gold_path}\n")
    return df_gold


# =============================================================================
# Enregistrement des tables dans le Metastore Databricks (SQL)
# =============================================================================

def register_tables_in_metastore():
    """
    Enregistre les tables Delta dans le catalogue SQL de Databricks.
    Permet d'interroger les données via SQL dans les notebooks.

    Utilisation après exécution :
        %sql SELECT * FROM gold_real_madrid_performance ORDER BY performance_rank;
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS football_db")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS football_db.bronze_real_madrid
        USING DELTA LOCATION '{BRONZE_PATH}'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS football_db.silver_real_madrid_players
        USING DELTA LOCATION '{SILVER_PATH}'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS football_db.gold_real_madrid_performance
        USING DELTA LOCATION '{GOLD_PATH}'
    """)

    print("✅ Tables enregistrées dans le catalogue SQL 'football_db'.")
    print("   → Utilisez : %sql SELECT * FROM football_db.gold_real_madrid_performance")


# =============================================================================
# PIPELINE COMPLET — Orchestration Bronze → Silver → Gold
# =============================================================================

def run_pipeline():
    """
    Lance le pipeline Medallion complet dans l'ordre :
    Bronze → Silver → Gold → Enregistrement Metastore
    """
    print("\n" + "=" * 60)
    print("  🚀 PIPELINE MEDALLION — Real Madrid Squad Stats")
    print("=" * 60 + "\n")

    # Étape 1 : Bronze
    create_bronze_layer(RAW_JSON_PATH, BRONZE_PATH)

    # Étape 2 : Silver
    create_silver_layer(BRONZE_PATH, SILVER_PATH)

    # Étape 3 : Gold
    create_gold_layer(SILVER_PATH, GOLD_PATH)

    # Étape 4 : Enregistrement dans le catalogue
    register_tables_in_metastore()

    print("\n" + "=" * 60)
    print("  ✅ Pipeline Medallion terminé avec succès !")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
