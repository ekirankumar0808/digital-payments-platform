import os
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from functools import reduce

from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger
from spark.utils.validators import *

env = os.getenv("ENV", "dev")
config = load_config(env)

logger = get_logger(
    logger_name="SilverTransformation",
    log_path=config['logging']['log_path'],
    log_level=config['logging']['log_level']
)

spark = create_spark_session("SilverTransformation")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

try:
    logger.info("Starting Silver Transformation Process")

    bronze_path = config['paths']['bronze']
    silver_path = config['paths']['silver']
    quarantine_path = config['paths']['quarantine']
    dq_path = config['paths']['dataquality']

    # ------------------------------------------------------
    # READ BRONZE
    # ------------------------------------------------------
    bronze_df = spark.read.format("delta").load(bronze_path)

    # ------------------------------------------------------
    # VALIDATION USING REUSABLE VALIDATORS
    # ------------------------------------------------------
    logger.info("Running Data Quality Validators")

    invalid_amount_df = validate_positive_amount(bronze_df)
    invalid_null_df = validate_null_transaction_id(bronze_df)
    invalid_balance_df = validate_balance(bronze_df)
    invalid_type_df = validate_transaction_type(bronze_df)

    bad_records_df = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        [invalid_amount_df, invalid_null_df, invalid_balance_df, invalid_type_df]
    ).dropDuplicates()

    # ------------------------------------------------------
    # QUARANTINE WRITE
    # ------------------------------------------------------
    logger.info("Writing Quarantine Records")

    bad_records_df.write \
        .format("delta") \
        .mode("append") \
        .save(quarantine_path)

    # ------------------------------------------------------
    # DATA QUALITY METRICS
    # ------------------------------------------------------
    logger.info("Writing QC Metrics")

    bad_records_df.cache()

    total_bad = bad_records_df.count()

    qc_metrics_df = bad_records_df.groupBy("validation_reason") \
        .count() \
        .withColumn("total_bad_records", lit(total_bad)) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("pipeline_stage", lit("silver"))

    qc_metrics_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(dq_path)

    # ------------------------------------------------------
    # CREATE SILVER BASE (REMOVE BAD RECORDS)
    # ------------------------------------------------------
    bronze_df = bronze_df.withColumn(
        "row_hash",
        sha2(concat_ws("||",
            col("step"),
            col("nameOrig"),
            col("nameDest"),
            col("amount").cast("string"),
            col("type")
        ), 256)
    )

    bad_records_df = bad_records_df.withColumn(
        "row_hash",
        sha2(concat_ws("||",
            col("step"),
            col("nameOrig"),
            col("nameDest"),
            col("amount").cast("string"),
            col("type")
        ), 256)
    )

    silver_base_df = bronze_df.join(
        bad_records_df.select("row_hash"),
        on="row_hash",
        how="left_anti"
    ).drop("row_hash")

    bad_records_df.unpersist()

    # ------------------------------------------------------
    # STANDARDIZATION
    # ------------------------------------------------------
    silver_df = silver_base_df \
        .dropDuplicates(["step", "nameOrig", "nameDest", "amount"]) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("oldbalanceOrg", col("oldbalanceOrg").cast("double")) \
        .withColumn("newbalanceOrig", col("newbalanceOrig").cast("double")) \
        .withColumn("oldbalanceDest", col("oldbalanceDest").cast("double")) \
        .withColumn("newbalanceDest", col("newbalanceDest").cast("double")) \
        .withColumn("isFraud", col("isFraud").cast("int")) \
        .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast("int")) \
        .withColumn("type", upper(col("type"))) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", col("ingestion_date")) \
        .withColumn("updated_timestamp", current_timestamp())

    # ------------------------------------------------------
    # FEATURE ENGINEERING
    # ------------------------------------------------------
    silver_df = silver_df \
        .withColumn("sender_balance_diff",
                    col("oldbalanceOrg") - col("newbalanceOrig")) \
        .withColumn("receiver_balance_diff",
                    col("newbalanceDest") - col("oldbalanceDest")) \
        .withColumn("is_full_depletion",
                    when(col("newbalanceOrig") == 0, 1).otherwise(0)) \
        .withColumn("is_high_value",
                    when(col("amount") > 200000, 1).otherwise(0)) \
        .withColumn("type_risk",
                    when(col("type") == "TRANSFER", 2)
                    .when(col("type") == "CASH_OUT", 2)
                    .otherwise(0)) \
        .withColumn("risk_score",
            col("isFraud") * 5 +
            col("isFlaggedFraud") * 4 +
            col("is_high_value") * 2 +
            col("is_full_depletion") * 2 +
            col("type_risk")
        )

    # ------------------------------------------------------
    # FINAL SILVER SCHEMA
    # ------------------------------------------------------
    silver_df = silver_df.select(
        "step", "type", "amount",
        "nameOrig", "oldbalanceOrg", "newbalanceOrig",
        "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud","ingestion_date",
        "sender_balance_diff", "receiver_balance_diff",
        "is_full_depletion", "is_high_value",
        "type_risk", "risk_score",
        "created_timestamp", "updated_timestamp", "source_file"
    )

    # ------------------------------------------------------
    # UPSERT KEY
    # ------------------------------------------------------
    silver_df = silver_df.withColumn(
        "transaction_key",
        sha2(
            concat_ws("||",
                col("step"),
                col("nameOrig"),
                col("nameDest"),
                col("amount").cast("string"),
                col("type")
            ),
            256
        )
    )

    merge_condition = "target.transaction_key = source.transaction_key"

    # ------------------------------------------------------
    # DELTA WRITE / MERGE
    # ------------------------------------------------------
    if DeltaTable.isDeltaTable(spark, silver_path):

        logger.info("Merging into existing Silver table")

        delta_table = DeltaTable.forPath(spark, silver_path)

        (
            delta_table.alias("target")
            .merge(
                silver_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdate(set={

                "type": "source.type",
                "amount": "source.amount",

                "oldbalanceOrg": "source.oldbalanceOrg",
                "newbalanceOrig": "source.newbalanceOrig",

                "oldbalanceDest": "source.oldbalanceDest",
                "newbalanceDest": "source.newbalanceDest",

                "isFraud": "source.isFraud",
                "isFlaggedFraud": "source.isFlaggedFraud",

                "sender_balance_diff": "source.sender_balance_diff",
                "receiver_balance_diff": "source.receiver_balance_diff",

                "is_full_depletion": "source.is_full_depletion",
                "is_high_value": "source.is_high_value",

                "type_risk": "source.type_risk",
                "risk_score": "source.risk_score",

                # preserve original creation time
                "created_timestamp": "target.created_timestamp",

                # update modification time
                "updated_timestamp": "source.updated_timestamp",

                "source_file": "source.source_file"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        logger.info("Creating Silver table")

        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .option("mergeSchema", "true") \
            .save(silver_path)

    logger.info("Silver transformation completed successfully")

    spark.stop()

except Exception as e:
    logger.error(f"Error in Silver layer: {str(e)}")
    spark.stop()
    raise e