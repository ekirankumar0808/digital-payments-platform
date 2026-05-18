import os
from datetime import datetime
from pyspark.sql.functions import *
from spark.utils.logger import get_logger
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session

try:
    env = os.getenv("ENV", "dev")
    config = load_config(env)

    logger = get_logger(
        logger_name="GoldAggregation",
        log_path=config['logging']['log_path'],
        log_level=config['logging']['log_level']
    )

    spark = create_spark_session("GoldAggregation")

    silver_path = config['paths']['silver']
    gold_path = config['paths']['gold']

    start_time = datetime.now()

    logger.info("Starting Gold Layer Aggregation for PaySim")

    # ------------------------------------------------------
    # READ SILVER
    # ------------------------------------------------------
    silver_df = spark.read.format("delta").load(silver_path)

    logger.info(f"Silver row count: {silver_df.count()}")

    # ------------------------------------------------------
    # GOLD AGGREGATION (FRAUD ANALYTICS)
    # ------------------------------------------------------
    gold_df = silver_df.groupBy("ingestion_date").agg(

        # total transactions
        count("*").alias("total_transactions"),

        # fraud KPIs
        sum("isFraud").alias("total_frauds"),
        sum("isFlaggedFraud").alias("total_flagged_frauds"),

        # fraud rate
        (sum("isFraud") / count("*")).alias("fraud_rate"),

        # financial exposure
        sum("amount").alias("total_transaction_value"),
        sum(when(col("isFraud") == 1, col("amount")).otherwise(0)).alias("fraud_amount"),

        # risk behavior
        sum("is_high_value").alias("high_value_txns"),
        sum("is_full_depletion").alias("full_depletion_events"),

        # transaction type behavior
        sum(when(col("type") == "CASH_OUT", 1).otherwise(0)).alias("cash_out_count"),
        sum(when(col("type") == "TRANSFER", 1).otherwise(0)).alias("transfer_count"),

        # risk score insights
        avg("risk_score").alias("avg_risk_score"),
        max("risk_score").alias("max_risk_score")
    )

    # ------------------------------------------------------
    # ADD METADATA
    # ------------------------------------------------------
    gold_df = gold_df.withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )

    gold_df = gold_df.withColumn(
        "fraud_ratio_percent",
        col("fraud_rate") * 100
    )

    gold_df = gold_df.withColumn(
        "year_month",
        date_format(col("ingestion_date"), "yyyy_MM")
    )

    logger.info(f"Gold row count: {gold_df.count()}")


    # ------------------------------------------------------
    # WRITE GOLD LAYER
    # ------------------------------------------------------
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .option("overwriteSchema", "true") \
        .save(gold_path)

    end_time = datetime.now()

    logger.info(
        f"Gold aggregation completed in {(end_time - start_time).total_seconds()} seconds"
    )

    spark.stop()

except Exception as e:
    logger.error(f"Error in Gold Layer: {str(e)}")
    spark.stop()
    raise e