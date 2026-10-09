"""
Pipeline COMPLETE - E-commerce_stream (client_e-commerce)
Tenant : client_e_commerce | Projet : e_commerce_stream
Consigne : Ingestion & Validation :
Lire un fichier CSV ou JSON de données brutes (orders_raw).
Valider la présence des champs obligatoires : order_id, customer_id, amount, status, created_at.
Filtrer ou corriger les anomalies (ex: rejeter les montants négatifs ou nuls, formater toutes les dates au format ISO 

Déploiement recommandé :
    python client_e_commerce_e_commerce_stream_pipeline.py bronze   # (streaming si Kafka : processus long, à garder actif)
    python client_e_commerce_e_commerce_stream_pipeline.py silver   # (batch, à planifier après Bronze)
    python client_e_commerce_e_commerce_stream_pipeline.py gold     # (batch, à planifier après Silver)
    python client_e_commerce_e_commerce_stream_pipeline.py all      # (exécute les 3 à la suite - pour un pipeline 100% batch)
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType, LongType, BooleanType
from pyspark.sql.functions import col, current_timestamp, trim, to_timestamp, window, count


def run_bronze(spark):
    BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/client_e_commerce/e_commerce_stream/bronze")
    BRONZE_DLQ_PATH = os.getenv("BRONZE_DLQ_PATH", "/data/client_e_commerce/e_commerce_stream/dlq/bronze")

    raw_schema = StructType([
        StructField("id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("payload", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    raw_df = (
        spark.read.format("jdbc")
        .option("url", os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/client_e_commerce_e_commerce_stream_db"))
        .option("dbtable", os.getenv("JDBC_TABLE", "raw_events"))
        .option("user", os.getenv("JDBC_USER", "changeme"))
        .option("password", os.getenv("JDBC_PASSWORD", "changeme"))
        .load()
    )

    # Tolérance aux erreurs : isole les lignes corrompues/invalides dans une DLQ
    # (Dead Letter Queue) au lieu de planter le job ou de les perdre silencieusement.
    if "_corrupt_record" in raw_df.columns:
        valid_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    else:
        valid_df = raw_df.filter(col("id").isNotNull() & col("event_time").isNotNull())
        corrupt_df = raw_df.filter(col("id").isNull() | col("event_time").isNull())

    bronze_df = valid_df.withColumn("ingestion_time", current_timestamp())

    bronze_df.write.format("delta").mode("append").save(BRONZE_PATH)

    if not corrupt_df.rdd.isEmpty():
        corrupt_df.write.format("delta").mode("append").save(BRONZE_DLQ_PATH)

    print(f"Bronze écrit : {bronze_df.count()} lignes valides, {corrupt_df.count()} lignes en quarantaine (DLQ), source=jdbc.")


def run_silver(spark):
    BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/client_e_commerce/e_commerce_stream/bronze")
    SILVER_PATH = os.getenv("SILVER_PATH", "/data/client_e_commerce/e_commerce_stream/silver")
    SILVER_DLQ_PATH = os.getenv("SILVER_DLQ_PATH", "/data/client_e_commerce/e_commerce_stream/dlq/silver")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    typed_df = (
        bronze_df
        .withColumn("event_type", trim(col("event_type")))
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    # Tolérance aux erreurs : les lignes qui échouent aux contrôles de qualité
    # (clé de dédoublonnage ou colonne temporelle manquante) sont isolées dans
    # une DLQ Silver plutôt que silencieusement supprimées ou plantant le job.
    quarantined_df = typed_df.filter(
        col("id").isNull() | col("event_time").isNull()
    )
    clean_df = (
        typed_df
        .filter(col("id").isNotNull() & col("event_time").isNotNull())
        .dropDuplicates(["id"])
    )

    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(SILVER_PATH)
    )

    if not quarantined_df.rdd.isEmpty():
        quarantined_df.write.format("delta").mode("append").save(SILVER_DLQ_PATH)

    print(f"Silver écrit : {clean_df.count()} lignes valides, {quarantined_df.count()} en quarantaine (dédoublonnage sur 'id').")


def run_gold(spark):
    SILVER_PATH = os.getenv("SILVER_PATH", "/data/client_e_commerce/e_commerce_stream/silver")
    GOLD_PATH = os.getenv("GOLD_PATH", "/data/client_e_commerce/e_commerce_stream/gold")
    WATERMARK_DELAY = os.getenv("GOLD_WATERMARK_DELAY", "2 hours")

    silver_df = spark.read.format("delta").load(SILVER_PATH)

    # Watermark : borne le retard toléré pour les événements tardifs. Indispensable
    # en streaming pour permettre à Spark de purger l'état des fenêtres et éviter
    # une croissance illimitée de la mémoire (state store).
    watermarked_df = silver_df.withWatermark("event_time", WATERMARK_DELAY)

    kpi_df = (
        watermarked_df
        .groupBy(
            window(col("event_time"), "1 hours"),
            col("event_type"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            "event_count",
        )
    )

    (
        kpi_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    print(f"Gold écrit : {kpi_df.count()} agrégations (count sur event_count, fenêtre 1 hours).")

    # --- Maintenance Delta Lake ---
    # OPTIMIZE / VACUUM sont coûteux : à exécuter en job planifié séparé (ex: 1x/jour),
    # pas à chaque exécution de la couche Gold. Désactivé par défaut.
    if os.getenv("RUN_DELTA_MAINTENANCE", "false").lower() == "true":
        spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}`")
        spark.sql(f"VACUUM delta.`{GOLD_PATH}` RETAIN 168 HOURS")


if __name__ == "__main__":
    STAGE = sys.argv[1] if len(sys.argv) > 1 else "all"

    spark = (
        SparkSession.builder
        .appName("client_e_commerce_e_commerce_stream_pipeline")
        .getOrCreate()
    )

    if STAGE in ("bronze", "all"):
        run_bronze(spark)
    if STAGE in ("silver", "all"):
        run_silver(spark)
    if STAGE in ("gold", "all"):
        run_gold(spark)
