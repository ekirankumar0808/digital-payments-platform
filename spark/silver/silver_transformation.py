import os
from delta.tables import DeltaTable
from pyspark.sql.functions  import *
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger
from spark.utils.validators import *

env = os.getenv("ENV", "dev")
config = load_config(env)

logger = get_logger(
    logger_name = "SilverTransformation",
    log_path =config['logging']['log_path'],
    log_level=config['logging']['log_level']
)

logger.info("Creating Spark Session with Delta Lake support")
spark = create_spark_session("SilverTransformation")

try:
    logger.info("Starting Silver Transformation Process")

    bronze_path = config['paths']['bronze']
    silver_path = config['paths']['silver']
    quarantine_path = config['paths']['quarantine']

    # Read Bronze Delta Table
    bronze_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    # ------------------------------------------------------
    #  Data Quality Validations
    # ------------------------------------------------------

    logger.info("Starting Data Quality Validations")

    invalid_amount_df = validate_positive_amount(bronze_df)

    invalid_transaction_df = (
        validate_null_transaction_id(bronze_df)
    )

    invalid_currency_df = validate_currency(bronze_df)

    # combine invalid records
    bad_records_df = (
        invalid_amount_df
        .union(invalid_transaction_df)
        .union(invalid_currency_df)
        .dropDuplicates()
    )

    # save the quarantine records
    logger.info("Saving quarantine records")
    (
        bad_records_df.write
        .format("delta")
        .mode("append")
        .save(quarantine_path)
    )

    # remove bad records from bronze_df
    silver_df = bronze_df.subtract(bad_records_df)


    #  Standardization

    silver_df = (
        silver_df
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "amount",
            col("amount").cast("double")
        )
        .withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_date"))
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        .withColumn(
            "transaction_date",
            to_date(col("transaction_timestamp"))
        )
        .withColumn(
        "status",
        upper(col("status"))
        )
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver Delta Table exists. Performing Merge for upsert.")
        silver_delta_table = DeltaTable.forPath(spark, silver_path)

        (
            silver_delta_table.alias("target")
            .merge(
                silver_df.alias("source"),
                "target.transaction_id = source.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    else:

        # Write Silver Delta Table
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("transaction_date")
            .save(silver_path)
        )

    logger.info("Silver Layer Upsert Operation Successfully")

    spark.stop()
except Exception as e:
    logger.error(f"Error during Silver Transformation: {str(e)}")
    spark.stop()
    raise e