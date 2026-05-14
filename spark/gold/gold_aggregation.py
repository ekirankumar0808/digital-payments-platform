from datetime import datetime
import os
from spark.utils.logger import get_logger
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    current_timestamp
)

try:
    env = os.getenv("ENV", "dev")
    config = load_config(env)


    logger = get_logger(
    logger_name = "GoldAggregation",
    log_path =config['logging']['log_path'],
    log_level=config['logging']['log_level']
)

    silver_path = config['paths']['silver']
    gold_path = config['paths']['gold']
    start_time = datetime.now()
    # Configure Spark Session
    spark = create_spark_session("GoldAggregation")

    logger.info("Starting Gold Aggregation Process")

    # Read Silver Delta Table
    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    logger.info(f"Silver Layer Row Count: {silver_df.count()}")


    # Gold Aggregations
    gold_df = (
        silver_df
        .groupBy("transaction_date")
        .agg(
            count("*").alias("total_transactions"),

            sum("amount").alias("total_revenue"),

            sum(
                when(
                    col("status") == "SUCCESS",
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            sum(
                when(
                    col("status") == "FAILED",
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )
    )

    # Add processing timestamp
    gold_df = gold_df.withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )

    logger.info(f"Gold Layer Row Count: {gold_df.count()}")

    # Write Gold Delta Table
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("transaction_date")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

    end_time = datetime.now()

    logger.info("Execution Time for Gold Aggregation: {:.2f} seconds".format((end_time - start_time).total_seconds()))

    spark.stop()

except Exception as e:
    logger.error(f"Error during Gold Aggregation: {str(e)}")
    spark.stop()
    raise e