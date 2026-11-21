import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg as spark_avg, max as spark_max,
    round as spark_round, least, lit, when, current_timestamp
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("MerchantRiskScoringJob")

def run_batch_risk_scoring():
    logger.info("Starting Spark Batch Merchant Risk Scoring job...")

    # PG
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "fraud_detection")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    db_properties = {
        "user": pg_user,
        "password" : pg_password,
        "driver": "org.postgresql.Driver"
    }

    # Spark
    spark = SparkSession.builder \
        .appName("FraudMerchantRiskScoring") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(f"Extracting alerts from {jdbc_url} (table: fraud_alerts)...")
        alerts_df = spark.read.jdbc(
            url=jdbc_url,
            table="fraud_alerts",
            properties=db_properties
        )

        total_input_rows = alerts_df.count()
        logger.info(f"Loaded {total_input_rows} alert records for processing")

        if total_input_rows == 0:
            logger.warning("No alert records found. Skipping computation.")
            return

        logger.info("Computing metrics by merchant_category...")
        aggregated_df = alerts_df.groupBy("merchant_category").agg(
            count("*").cast("bigint").alias("total_alerts"),
            spark_round(spark_sum("amount"), 2).alias("total_fraud_volume"),
            spark_round(spark_avg("amount"), 2).alias("avg_fraud_amount"),
            spark_round(spark_max("amount"), 2).alias("max_fraud_amount")
        )

        # Расчёт скоринга
        # Формула скоринга 0-100 баллов:
        # - Частота инцидентов: min(total_alerts * 5.0, 40.0) -> максимум 40 баллов
        # - Суммарный объем: min((total_fraud_volume / 100000.0) * 15.0, 40.0) -> максимум 40 баллов
        # - Средний чек: min((avg_fraud_amount / 20000.0) * 20.0, 20.0) -> максимум 20 баллов
        freq_score = least(col("total_alerts") * 5.0, lit(40.0))
        vol_score = least((col("total_fraud_volume") / 100000.0) * 15.0, lit(40.0))
        avg_score = least((col("avg_fraud_amount") / 20000.0) * 20.0, lit(20.0))

        raw_score = spark_round(freq_score + vol_score + avg_score, 2)
        final_score = least(raw_score, lit(100.0))

        scored_df = aggregated_df \
            .withColumn("risk_score", final_score) \
            .withColumn(
                "risk_level",
                when(col("risk_score") >= 80.0, "CRITICAL")
                .when(col("risk_score") >= 60.0, "HIGH")
                .when(col("risk_score") >= 40.0, "MEDIUM")
                .otherwise("LOW")
        ) \
        .withColumn("last_updated_at", current_timestamp())

        logger.info("Scoring completed. Summary preview:")
        scored_df.show(truncate=False)

        # Запись в витрину
        logger.info("Writing metrics to PostgreSQL (table: merchant_risk_metrics)...")
        scored_df.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(url=jdbc_url, table="merchant_risk_metrics", properties=db_properties)

        logger.info("Batch risk scoring job completed successfully.")

    except Exception as e:
        logger.error(f"Error during risk scoring execution: {str(e)}", exc_info=True)
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_batch_risk_scoring()