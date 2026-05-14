import os

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    current_date
)
from spark.schemas.transaction_schema import transaction_schema
from spark.utils.config_loader import load_config
from spark.utils.logger import get_logger
from spark.utils.spark_session import create_spark_session


try:

    env = os.getenv("ENV", "dev")

    # Load config
    config = load_config(env)

    # Logger
    logger = get_logger(
        logger_name="BronzeIngestion",
        log_path=config['logging']['log_path'],
        log_level=config['logging']['log_level']
    )

    raw_path = config['paths']['raw']
    bronze_path = config['paths']['bronze']

    logger.info("Starting Bronze Ingestion Process")

    # Create Spark Session
    spark = create_spark_session("BronzeIngestion")

    # Reduce Spark Logs
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Reading Raw CSV Data")

    # Read CSV
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("schema", transaction_schema)
        .load(os.path.join(raw_path, "transactions.csv"))
    )

    input_count = df.count()

    logger.info(f"Input Row Count: {input_count}")

    logger.info("Adding Metadata Columns")

    # Add metadata columns
    df = (
        df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_file",
            input_file_name()
        )
        .withColumn(
            "ingestion_date",
            current_date()
        )
    )

    logger.info("Writing Bronze Delta Table")

    # Delta write with schema evolution
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    logger.info("Bronze Layer Created Successfully")

    output_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    logger.info(
        f"Bronze Total Row Count: {output_df.count()}"
    )

    logger.info("Stopping Spark Session")

    spark.stop()

except Exception as e:

    logger.error(
        f"Error During Bronze Ingestion: {str(e)}"
    )

    if 'spark' in locals():
        spark.stop()

    raise e