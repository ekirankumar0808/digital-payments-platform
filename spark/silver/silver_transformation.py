import os
import uuid
from functools import reduce
from datetime import datetime

from delta.tables import DeltaTable

from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    upper,
    when,
    max as spark_max
    , concat_ws, collect_set
)

from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger
from spark.utils.validators import *
from spark.utils.audit_logger import write_audit_log


class SilverTransformationJob:

    def __init__(self):

        self.env = os.getenv("ENV", "dev")
        self.config = load_config(self.env)

        self.spark = create_spark_session("SilverTransformation")
        self.spark.sparkContext.setLogLevel("ERROR")

        # ---------------------------------------------------------
        # DELTA OPTIMIZATIONS
        # ---------------------------------------------------------

        self.spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled",
            "true"
        )

        self.spark.conf.set(
            "spark.databricks.delta.autoCompact.enabled",
            "true"
        )

        # ---------------------------------------------------------
        # LOGGER
        # ---------------------------------------------------------

        self.logger = get_logger(
            logger_name="SilverTransformation",
            log_path=self.config['logging']['log_path'],
            log_level=self.config['logging']['log_level']
        )

        # ---------------------------------------------------------
        # PATHS
        # ---------------------------------------------------------

        self.bronze_path = self.config['paths']['bronze']
        self.silver_path = self.config['paths']['silver']
        self.quarantine_path = self.config['paths']['quarantine']
        self.silver_metadata_path = self.config['paths']['silver_metadata']
        self.audit_path = self.config['paths']['audit_pipeline_runs']
        self.validation_metrics_path = self.config['paths']['validation_metrics']

        self.spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true"
        )

    # ---------------------------------------------------------
    # READ INCREMENTAL BRONZE DATA
    # ---------------------------------------------------------

    def get_incremental_bronze_data(self):

        self.logger.info("Reading incremental Bronze records")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(self.bronze_path)
        )

        try:

            metadata_df = (
                self.spark.read
                .format("delta")
                .load(self.silver_metadata_path)
            )

            last_processed_timestamp = metadata_df.select(
                spark_max("last_processed_timestamp")
            ).collect()[0][0]

            self.logger.info(
                f"Last processed timestamp: "
                f"{last_processed_timestamp}"
            )

            incremental_df = bronze_df.filter(
                col("ingestion_timestamp") >= lit(last_processed_timestamp)
            )

        except Exception as e:

            self.logger.warning(
                f"No watermark found. "
                f"Full load triggered. Reason: {str(e)}"
            )

            incremental_df = bronze_df

        return incremental_df

    # ---------------------------------------------------------
    # VALIDATIONS
    # ---------------------------------------------------------

    def run_validations(self, bronze_df):

        invalid_amount_df = validate_positive_amount(bronze_df)

        invalid_null_df = validate_null_transaction_id(
            bronze_df
        )

        invalid_balance_df = validate_balance(
            bronze_df
        )

        invalid_type_df = validate_transaction_type(
            bronze_df
        )

        combined_invalid_df = reduce(
            lambda df1, df2: df1.unionByName(
                df2,
                allowMissingColumns=True
            ),
            [
                invalid_amount_df,
                invalid_null_df,
                invalid_balance_df,
                invalid_type_df
            ]
        )

        bad_records_df = (
            combined_invalid_df
            .groupBy(
                *[
                    column_name
                    for column_name in combined_invalid_df.columns
                    if column_name != "validation_reason"
                ]
            )
            .agg(
                concat_ws(",", collect_set("validation_reason"))
                .alias("validation_reason")
            )
        )

        return bad_records_df

    # ---------------------------------------------------------
    # WRITE QUARANTINE
    # ---------------------------------------------------------

    def write_quarantine(self, bad_records_df):

        if bad_records_df.limit(1).count() == 0:

            self.logger.info(
                "No invalid records found"
            )

            return

        (
            bad_records_df
            .repartition(2)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.quarantine_path)
        )

    # ---------------------------------------------------------
    # REMOVE BAD RECORDS
    # ---------------------------------------------------------

    def remove_bad_records(
        self,
        bronze_df,
        bad_records_df
    ):

        return bronze_df.join(
            bad_records_df.select("transaction_id"),
            on="transaction_id",
            how="left_anti"
        )

    # ---------------------------------------------------------
    # TRANSFORM SILVER
    # ---------------------------------------------------------

    def transform_data(self, silver_base_df):

        silver_df = (

            silver_base_df

            # -------------------------------------------------
            # DEDUPLICATION
            # -------------------------------------------------

            .dropDuplicates(["transaction_id"])

            # -------------------------------------------------
            # RENAME COLUMNS
            # -------------------------------------------------

            .withColumnRenamed(
                "type",
                "transaction_type"
            )

            .withColumnRenamed(
                "nameOrig",
                "sender_id"
            )

            .withColumnRenamed(
                "nameDest",
                "receiver_id"
            )

            # -------------------------------------------------
            # STANDARDIZATION
            # -------------------------------------------------

            .withColumn(
                "transaction_type",
                upper(col("transaction_type"))
            )

            # -------------------------------------------------
            # CASTING
            # -------------------------------------------------

            .withColumn(
                "step",
                col("step").cast("int")
            )

            .withColumn(
                "amount",
                col("amount").cast("double")
            )

            .withColumn(
                "oldbalanceOrg",
                col("oldbalanceOrg").cast("double")
            )

            .withColumn(
                "newbalanceOrig",
                col("newbalanceOrig").cast("double")
            )

            .withColumn(
                "oldbalanceDest",
                col("oldbalanceDest").cast("double")
            )

            .withColumn(
                "newbalanceDest",
                col("newbalanceDest").cast("double")
            )

            .withColumn(
                "isFraud",
                col("isFraud").cast("int")
            )

            .withColumn(
                "isFlaggedFraud",
                col("isFlaggedFraud").cast("int")
            )

            # -------------------------------------------------
            # SILVER PROCESSING TIMESTAMP
            # -------------------------------------------------

            .withColumn(
                "silver_processing_timestamp",
                current_timestamp()
            )

            # -------------------------------------------------
            # FEATURE ENGINEERING
            # -------------------------------------------------

            .withColumn(
                "sender_balance_diff",
                col("oldbalanceOrg")
                - col("newbalanceOrig")
            )

            .withColumn(
                "receiver_balance_diff",
                col("newbalanceDest")
                - col("oldbalanceDest")
            )

            .withColumn(
                "is_full_depletion",
                when(
                    col("newbalanceOrig") == 0,
                    1
                ).otherwise(0)
            )

            .withColumn(
                "is_high_value",
                when(
                    col("amount") > 200000,
                    1
                ).otherwise(0)
            )

            .withColumn(
                "type_risk",
                when(
                    col("transaction_type")
                    == "TRANSFER",
                    2
                )
                .when(
                    col("transaction_type")
                    == "CASH_OUT",
                    2
                )
                .otherwise(0)
            )
        )

        # ---------------------------------------------------------
        # RISK SCORE
        # ---------------------------------------------------------

        silver_df = silver_df.withColumn(
            "risk_score",
            col("isFraud") * 5
            + col("isFlaggedFraud") * 4
            + col("is_high_value") * 2
            + col("is_full_depletion") * 2
            + col("type_risk")
        )

        return silver_df

    # ---------------------------------------------------------
    # UPSERT SILVER
    # ---------------------------------------------------------

    def upsert_silver(self, silver_df):

        silver_df = silver_df.repartition(4)

        if DeltaTable.isDeltaTable(
            self.spark,
            self.silver_path
        ):

            delta_table = DeltaTable.forPath(
                self.spark,
                self.silver_path
            )

            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.transaction_id = "
                    "source.transaction_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("ingestion_date")
                .option("mergeSchema", "true")
                .save(self.silver_path)
            )

    # ---------------------------------------------------------
    # UPDATE WATERMARK
    # ---------------------------------------------------------

    def update_watermark(self, bronze_df):

        latest_timestamp = bronze_df.select(
            spark_max("ingestion_timestamp")
        ).collect()[0][0]

        watermark_df = self.spark.createDataFrame(
            [(latest_timestamp,)],
            ["last_processed_timestamp"]
        )

        (
            watermark_df.write
            .format("delta")
            .mode("overwrite")
            .save(self.silver_metadata_path)
        )

    # ---------------------------------------------------------
    # WRITE VALIDATION METRICS
    # ---------------------------------------------------------

    def write_validation_metrics(
        self,
        pipeline_run_id,
        total_records,
        valid_records,
        invalid_records
        ,
        failure_rate
    ):

        metrics_schema = StructType([
            StructField("pipeline_run_id", StringType(), False),
            StructField("total_records", LongType(), False),
            StructField("valid_records", LongType(), True),
            StructField("invalid_records", LongType(), True),
            StructField("failure_rate", DoubleType(), True),
            StructField("aggregated_records", LongType(), True),
            StructField("metric_type", StringType(), False),
            StructField("created_timestamp", TimestampType(), False)
        ])

        metrics_df = self.spark.createDataFrame(
            [
                (
                    pipeline_run_id,
                    total_records,
                    valid_records,
                    invalid_records,
                    failure_rate,
                    None,
                    "silver",
                    datetime.now()
                )
            ],
            schema=metrics_schema
        )

        (
            metrics_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.validation_metrics_path)
        )

    # ---------------------------------------------------------
    # RUN
    # ---------------------------------------------------------

    def run(self):

        pipeline_run_id = str(uuid.uuid4())

        try:

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Silver transformation started"
            )

            bronze_df = (
                self.get_incremental_bronze_data()
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            # -------------------------------------------------
            # EMPTY CHECK
            # -------------------------------------------------

            if bronze_df.limit(1).count() == 0:

                self.logger.info(
                    f"[RUN_ID={pipeline_run_id}] "
                    f"No records found"
                )

                bronze_df.unpersist()

                return

            # -------------------------------------------------
            # VALIDATIONS
            # -------------------------------------------------

            bad_records_df = (
                self.run_validations(bronze_df)
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            # -------------------------------------------------
            # METRICS
            # -------------------------------------------------

            total_records = bronze_df.count()

            invalid_records = bad_records_df.count()

            valid_records = (
                total_records - invalid_records
            )

            failure_rate = (
                invalid_records / total_records
            ) if total_records > 0 else 0.0

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Failure Rate: {failure_rate:.6f}"
            )

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Total Records: {total_records}"
            )

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Valid Records: {valid_records}"
            )

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Invalid Records: {invalid_records}"
            )

            # -------------------------------------------------
            # QUARANTINE
            # -------------------------------------------------

            self.write_quarantine(bad_records_df)

            # -------------------------------------------------
            # REMOVE BAD RECORDS
            # -------------------------------------------------

            silver_base_df = self.remove_bad_records(
                bronze_df,
                bad_records_df
            )

            # -------------------------------------------------
            # TRANSFORM
            # -------------------------------------------------

            silver_df = self.transform_data(
                silver_base_df
            )

            # -------------------------------------------------
            # UPSERT SILVER
            # -------------------------------------------------

            self.upsert_silver(silver_df)

            # -------------------------------------------------
            # WATERMARK
            # -------------------------------------------------

            self.update_watermark(bronze_df)

            # -------------------------------------------------
            # VALIDATION METRICS
            # -------------------------------------------------

            self.write_validation_metrics(
                pipeline_run_id=pipeline_run_id,
                total_records=total_records,
                valid_records=valid_records,
                invalid_records=invalid_records
                ,failure_rate=failure_rate
            )

            # -------------------------------------------------
            # AUDIT LOG
            # -------------------------------------------------

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="silver",
                run_id=pipeline_run_id,
                status="SUCCESS",
                records_processed=valid_records
            )

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Silver transformation completed"
            )

            bronze_df.unpersist()
            bad_records_df.unpersist()

        except Exception as e:

            self.logger.error(
                f"[RUN_ID={pipeline_run_id}] "
                f"Silver transformation failed: {str(e)}"
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="silver",
                run_id=pipeline_run_id,
                status="FAILED",
                records_processed=0,
                error_message=str(e)
            )

            raise e

        finally:

            self.spark.stop()


if __name__ == "__main__":

    job = SilverTransformationJob()

    job.run()
