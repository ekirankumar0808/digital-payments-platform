import os
from functools import reduce
from datetime import datetime

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    current_date,
    upper,
    when,
    max as spark_max,
    sha2,
    concat_ws
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

        self.spark = create_spark_session(
            "SilverTransformation"
        )

        self.spark.sparkContext.setLogLevel(
            "ERROR"
        )

        # ---------------------------------------------------------
        # DELTA OPTIMIZATION
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

        self.dq_path = self.config['paths']['dataquality']

        self.silver_metadata_path = (
            self.config['paths']['silver_metadata']
        )

        self.audit_path = (
            self.config['paths']['audit_pipeline_runs']
        )

        self.validation_metrics_path = (
            self.config['paths']['validation_metrics']
        )

    # ---------------------------------------------------------
    # READ INCREMENTAL BRONZE DATA
    # ---------------------------------------------------------

    def get_incremental_bronze_data(self):

        self.logger.info(
            "Reading incremental Bronze records"
        )

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(self.bronze_path)
        )

        # ---------------------------------------------------------
        # GET LAST WATERMARK
        # ---------------------------------------------------------

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
                col("ingestion_timestamp") >
                lit(last_processed_timestamp)
            )

        except Exception as e:

            self.logger.warning(
                f"No Silver watermark found. "
                f"Full Bronze load will run. "
                f"Reason: {str(e)}"
            )

            incremental_df = bronze_df

        return incremental_df

    # ---------------------------------------------------------
    # VALIDATIONS
    # ---------------------------------------------------------

    def run_validations(self, bronze_df):

        self.logger.info(
            "Running Data Quality Validators"
        )

        invalid_amount_df = validate_positive_amount(
            bronze_df
        )

        invalid_null_df = validate_null_transaction_id(
            bronze_df
        )

        invalid_balance_df = validate_balance(
            bronze_df
        )

        invalid_type_df = validate_transaction_type(
            bronze_df
        )

        bad_records_df = reduce(
            lambda df1, df2:
            df1.unionByName(
                df2,
                allowMissingColumns=True
            ),
            [
                invalid_amount_df,
                invalid_null_df,
                invalid_balance_df,
                invalid_type_df
            ]
        ).dropDuplicates([
            "transaction_id"
        ])

        return bad_records_df

    # ---------------------------------------------------------
    # WRITE QUARANTINE
    # ---------------------------------------------------------

    def write_quarantine(self, bad_records_df):

        self.logger.info(
            "Writing Quarantine records"
        )

        (
            bad_records_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.quarantine_path)
        )

    # ---------------------------------------------------------
    # DATA QUALITY METRICS
    # ---------------------------------------------------------

    def write_data_quality_metrics(
        self,
        bad_records_df
    ):

        self.logger.info(
            "Writing QC metrics"
        )

        bad_records_df.cache()

        total_bad = bad_records_df.count()

        qc_metrics_df = (
            bad_records_df.groupBy(
                "validation_reason"
            )
            .count()
            .withColumn(
                "total_bad_records",
                lit(total_bad)
            )
            .withColumn(
                "ingestion_date",
                current_date()
            )
            .withColumn(
                "pipeline_stage",
                lit("silver")
            )
        )

        (
            qc_metrics_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.dq_path)
        )

        bad_records_df.unpersist()

    # ---------------------------------------------------------
    # VALIDATION METRICS
    # ---------------------------------------------------------

    def write_validation_metrics(
        self,
        total_records,
        valid_records,
        invalid_records,
        pipeline_run_id
    ):

        metrics_df = self.spark.createDataFrame([
            (
                pipeline_run_id,
                total_records,
                valid_records,
                invalid_records,
                "silver",
                datetime.now()
            )
        ], [
            "pipeline_run_id",
            "total_records",
            "valid_records",
            "invalid_records",
            "pipeline_stage",
            "created_timestamp"
        ])

        (
            metrics_df.write
            .format("delta")
            .mode("append")
            .save(self.validation_metrics_path)
        )

        self.logger.info(
            "Validation metrics written successfully"
        )

    # ---------------------------------------------------------
    # REMOVE BAD RECORDS
    # ---------------------------------------------------------

    def remove_bad_records(
        self,
        bronze_df,
        bad_records_df
    ):

        silver_base_df = bronze_df.join(
            bad_records_df.select(
                "transaction_id"
            ),
            on="transaction_id",
            how="left_anti"
        )

        return silver_base_df

    # ---------------------------------------------------------
    # TRANSFORMATIONS
    # ---------------------------------------------------------

    def transform_data(self, silver_base_df):

        silver_df = (
            silver_base_df

            .withColumn(
                "transaction_id",
                sha2(
                    concat_ws(
                        "||",
                        col("step").cast("string"),
                        col("type"),
                        col("nameOrig"),
                        col("nameDest"),
                        col("amount").cast("string")
                    ),
                    256
                )
            )

            .dropDuplicates([
                "transaction_id"
            ])

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

            .withColumn(
                "transaction_type",
                upper(col("transaction_type"))
            )

            .withColumn(
                "silver_ingestion_timestamp",
                current_timestamp()
            )

            .withColumn(
                "updated_timestamp",
                current_timestamp()
            )
        )

        silver_df = (
            silver_df

            .withColumn(
                "sender_balance_diff",
                col("oldbalanceOrg") -
                col("newbalanceOrig")
            )

            .withColumn(
                "receiver_balance_diff",
                col("newbalanceDest") -
                col("oldbalanceDest")
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
                    col("transaction_type") == "TRANSFER",
                    2
                ).when(
                    col("transaction_type") == "CASH_OUT",
                    2
                ).otherwise(0)
            )
        )

        silver_df = silver_df.withColumn(
            "risk_score",
            col("isFraud") * 5 +
            col("isFlaggedFraud") * 4 +
            col("is_high_value") * 2 +
            col("is_full_depletion") * 2 +
            col("type_risk")
        )

        return silver_df

    # ---------------------------------------------------------
    # UPSERT SILVER
    # ---------------------------------------------------------

    def upsert_silver(self, silver_df):

        silver_df = silver_df.repartition(4)

        merge_condition = (
            "target.transaction_id = "
            "source.transaction_id"
        )

        if DeltaTable.isDeltaTable(
            self.spark,
            self.silver_path
        ):

            self.logger.info(
                "Merging into existing Silver table"
            )

            delta_table = DeltaTable.forPath(
                self.spark,
                self.silver_path
            )

            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            self.logger.info(
                "Creating Silver table"
            )

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

        self.logger.info(
            f"Updated Silver watermark: "
            f"{latest_timestamp}"
        )

    # ---------------------------------------------------------
    # MAIN EXECUTION
    # ---------------------------------------------------------

    def run(self):

        try:

            self.logger.info(
                "Starting Silver Transformation"
            )

            pipeline_run_id = str(datetime.now())

            bronze_df = self.get_incremental_bronze_data()

            if bronze_df.limit(1).count() == 0:

                self.logger.info(
                    "No new Bronze records found"
                )

                write_audit_log(
                    spark=self.spark,
                    audit_path=self.audit_path,
                    pipeline_name="digital_payments_pipeline",
                    layer="silver",
                    run_id=pipeline_run_id,
                    status="SUCCESS",
                    records_processed=0,
                    error_message="No incremental bronze records found"
                )

                self.spark.stop()

                return

            bad_records_df = self.run_validations(
                bronze_df
            )

            total_records = bronze_df.count()

            invalid_records = bad_records_df.count()

            valid_records = total_records - invalid_records

            self.write_quarantine(
                bad_records_df
            )

            self.write_data_quality_metrics(
                bad_records_df
            )

            self.write_validation_metrics(
                total_records=total_records,
                valid_records=valid_records,
                invalid_records=invalid_records,
                pipeline_run_id=pipeline_run_id
            )

            silver_base_df = self.remove_bad_records(
                bronze_df,
                bad_records_df
            )

            silver_df = self.transform_data(
                silver_base_df
            )

            self.upsert_silver(
                silver_df
            )

            self.update_watermark(
                bronze_df
            )

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
                "Silver Transformation completed"
            )

            self.spark.stop()

        except Exception as e:

            self.logger.error(
                f"Error in Silver Layer: {str(e)}"
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="silver",
                run_id=str(datetime.now()),
                status="FAILED",
                records_processed=0,
                error_message=str(e)
            )

            self.spark.stop()

            raise e


# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------

if __name__ == "__main__":

    job = SilverTransformationJob()

    job.run()