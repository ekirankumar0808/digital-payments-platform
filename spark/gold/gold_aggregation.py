import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark import StorageLevel

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    max as spark_max,
    when,
    current_timestamp,
    date_format,
    lit,
    to_date
)

from spark.utils.logger import get_logger
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.audit_logger import write_audit_log


class GoldAggregationJob:

    def __init__(self):

        self.env = os.getenv("ENV", "dev")
        self.config = load_config(self.env)

        self.spark = create_spark_session("GoldAggregation")
        self.spark.sparkContext.setLogLevel("ERROR")

        # Delta optimizations
        self.spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled",
            "true"
        )

        self.spark.conf.set(
            "spark.databricks.delta.autoCompact.enabled",
            "true"
        )

        self.logger = get_logger(
            logger_name="GoldAggregation",
            log_path=self.config['logging']['log_path'],
            log_level=self.config['logging']['log_level']
        )

        # Paths
        self.silver_path = self.config['paths']['silver']
        self.gold_path = self.config['paths']['gold']
        self.gold_metadata_path = self.config['paths']['gold_metadata']
        self.audit_path = self.config['paths']['audit_pipeline_runs']
        self.validation_metrics_path = self.config['paths']['validation_metrics']

    # ------------------------------------------------------
    # READ INCREMENTAL SILVER
    # ------------------------------------------------------

    def get_incremental_silver_data(self):

        self.logger.info("Reading incremental Silver data")

        silver_df = (
            self.spark.read
            .format("delta")
            .load(self.silver_path)
        )

        try:

            watermark_df = (
                self.spark.read
                .format("delta")
                .load(self.gold_metadata_path)
            )

            last_processed_timestamp = watermark_df.selectExpr(
                "max(last_processed_timestamp)"
            ).collect()[0][0]

            self.logger.info(
                f"Last Gold watermark: {last_processed_timestamp}"
            )

            silver_df = silver_df.filter(
                col("silver_processing_timestamp") >
                lit(last_processed_timestamp)
            )

        except Exception as e:

            self.logger.warning(
                f"No Gold watermark found. Full aggregation. Reason: {str(e)}"
            )

        return silver_df

    # ------------------------------------------------------
    # GOLD AGGREGATION
    # ------------------------------------------------------

    def aggregate_metrics(self, silver_df):

        self.logger.info("Running Gold aggregations")

        silver_df = silver_df.repartition(4)

        silver_df.persist(StorageLevel.MEMORY_AND_DISK)

        gold_df = (
            silver_df.groupBy("ingestion_date")
            .agg(

                count("*").alias("total_transactions"),

                countDistinct("sender_id").alias(
                    "unique_senders"
                ),

                countDistinct("receiver_id").alias(
                    "unique_receivers"
                ),

                sum("isFraud").alias(
                    "total_frauds"
                ),

                sum("isFlaggedFraud").alias(
                    "total_flagged_frauds"
                ),

                (
                    sum("isFraud").cast("double")
                    / count("*")).alias("fraud_rate"
                ),

                sum("amount").alias(
                    "total_transaction_value"
                ),

                sum(
                    when(
                        col("isFraud") == 1,
                        col("amount")
                    ).otherwise(0)
                ).alias(
                    "fraud_amount"
                ),

                avg(
                    when(
                        col("isFraud") == 1,
                        col("amount")
                    ).otherwise(0)
                ).alias(
                    "avg_fraud_amount"
                ),

                sum("is_high_value").alias(
                    "high_value_txns"
                ),

                sum("is_full_depletion").alias(
                    "full_depletion_events"
                ),

                sum(
                    when(
                        col("transaction_type") == "CASH_OUT",
                        1
                    ).otherwise(0)
                ).alias(
                    "cash_out_count"
                ),

                sum(
                    when(
                        col("transaction_type") == "TRANSFER",
                        1
                    ).otherwise(0)
                ).alias(
                    "transfer_count"
                ),

                avg("risk_score").alias(
                    "avg_risk_score"
                ),

                spark_max("risk_score").alias(
                    "max_risk_score"
                )
            )
        )

        silver_df.unpersist()

        gold_df = (
            gold_df
            .withColumn(
                "gold_processed_timestamp",
                current_timestamp()
            )
            .withColumn(
                "fraud_ratio_percent",
                col("fraud_rate") * 100
            )
            .withColumn(
                "year_month",
                date_format(
                    to_date(col("ingestion_date")),
                    "yyyy_MM"
                )
            )
        )

        return gold_df

    # ------------------------------------------------------
    # UPSERT GOLD TABLE
    # ------------------------------------------------------

    def upsert_gold_table(self, gold_df):

        gold_df = gold_df.repartition(4)

        merge_condition = (
            "target.ingestion_date = source.ingestion_date"
        )

        if DeltaTable.isDeltaTable(
            self.spark,
            self.gold_path
        ):

            self.logger.info("Merging into Gold table")

            delta_table = DeltaTable.forPath(
                self.spark,
                self.gold_path
            )

            (
                delta_table.alias("target")
                .merge(
                    gold_df.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            self.logger.info("Creating Gold table")

            (
                gold_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("year_month")
                .option("mergeSchema", "true")
                .save(self.gold_path)
            )

    # ------------------------------------------------------
    # UPDATE WATERMARK
    # ------------------------------------------------------

    def update_gold_watermark(self, silver_df):

        latest_timestamp = silver_df.selectExpr(
            "max(silver_processing_timestamp)"
        ).collect()[0][0]

        watermark_df = self.spark.createDataFrame(
            [(latest_timestamp,)],
            ["last_processed_timestamp"]
        )

        (
            watermark_df.write
            .format("delta")
            .mode("overwrite")
            .save(self.gold_metadata_path)
        )

        self.logger.info(
            f"Updated Gold watermark: {latest_timestamp}"
        )

    # ------------------------------------------------------
    # VALIDATION METRICS
    # ------------------------------------------------------

    def write_validation_metrics(
        self,
        total_records,
        aggregated_records,
        pipeline_run_id
    ):

        metrics_df = self.spark.createDataFrame(
            [(
                pipeline_run_id,
                total_records,
                None,
                None,
                None,
                aggregated_records,
                "gold",
                datetime.now()
            )],
            [
                "pipeline_run_id",
                "total_records",
                "valid_records",
                "invalid_records",
                "failure_rate",
                "aggregated_records",
                "metric_type",
                "created_timestamp"
            ]
        )

        (
            metrics_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.validation_metrics_path)
        )

        self.logger.info(
            "Validation metrics written successfully"
        )

    # ------------------------------------------------------
    # MAIN EXECUTION
    # ------------------------------------------------------

    def run(self):

        try:

            pipeline_run_id = str(datetime.now())

            self.logger.info(
                "Starting Gold Aggregation"
            )

            silver_df = self.get_incremental_silver_data()

            if silver_df.limit(1).count() == 0:

                self.logger.info(
                    "No new Silver records found"
                )

                write_audit_log(
                    spark=self.spark,
                    audit_path=self.audit_path,
                    pipeline_name="digital_payments_pipeline",
                    layer="gold",
                    run_id=pipeline_run_id,
                    status="SUCCESS",
                    records_processed=0,
                    error_message="No incremental silver records found"
                )

                self.spark.stop()
                return

            total_records = silver_df.count()

            gold_df = self.aggregate_metrics(
                silver_df
            )

            aggregated_records = gold_df.count()

            self.upsert_gold_table(gold_df)

            self.update_gold_watermark(silver_df)

            self.write_validation_metrics(
                total_records=total_records,
                aggregated_records=aggregated_records,
                pipeline_run_id=pipeline_run_id
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="gold",
                run_id=pipeline_run_id,
                status="SUCCESS",
                records_processed=aggregated_records
            )

            self.logger.info(
                "Gold aggregation completed successfully"
            )

            self.spark.stop()

        except Exception as e:

            self.logger.error(
                f"Error in Gold Layer: {str(e)}"
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="gold",
                run_id=str(datetime.now()),
                status="FAILED",
                records_processed=0,
                error_message=str(e)
            )

            self.spark.stop()

            raise e


if __name__ == "__main__":

    job = GoldAggregationJob()

    job.run()