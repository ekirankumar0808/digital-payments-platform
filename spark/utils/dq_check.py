import os
from delta.tables import DeltaTable
from pyspark.sql.functions import col, desc
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger


def table_exists(spark, path):
    """Return True when a Delta table exists at the configured path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def get_latest_validation_metrics(spark, validation_metrics_path):
    """Read the latest silver validation_metrics record for DQ evaluation."""
    metrics_df = (
        spark.read
        .format("delta")
        .load(validation_metrics_path)
    )

    # Only evaluate Silver layer metrics for DQ monitoring.
    if "metric_type" in metrics_df.columns:
        metrics_df = metrics_df.filter(col("metric_type") == "silver")

    metrics_df = metrics_df.orderBy(desc("created_timestamp"))

    if metrics_df.limit(1).count() == 0:
        return None

    return metrics_df.limit(1).collect()[0]


def parse_threshold(config):
    """Read and validate the configurable failure threshold."""
    threshold = float(config.get("dq", {}).get("failure_threshold", 0.05))
    if threshold < 0 or threshold > 1:
        raise ValueError("dq.failure_threshold must be between 0.0 and 1.0")
    return threshold


def main():
    env = os.getenv("ENV", "dev")
    config = load_config(env)

    logger = get_logger(
        logger_name="DataQualityCheck",
        log_path=config["logging"]["log_path"],
        log_level=config["logging"]["log_level"]
    )

    spark = create_spark_session("DataQualityCheck")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    validation_metrics_path = config["paths"]["validation_metrics"]

    try:
        logger.info("Starting Data Quality Check")

        if not table_exists(spark, validation_metrics_path):
            logger.warning(
                "Validation metrics table not found; "
                "skipping DQ threshold evaluation. "
                "Ensure Silver transformation writes metrics before DQ runs."
            )
            return

        threshold = parse_threshold(config)
        logger.info(f"DQ threshold configured at {threshold:.2%}")

        latest_record = get_latest_validation_metrics(
            spark,
            validation_metrics_path
        )

        if latest_record is None:
            logger.warning(
                "No Silver validation metrics found in the metrics table. "
                "DQ check will exit cleanly but the upstream run should be reviewed."
            )
            return

        latest_dict = latest_record.asDict()
        pipeline_run_id = latest_dict["pipeline_run_id"]
        total_records = latest_dict["total_records"]
        valid_records = latest_dict.get("valid_records")
        invalid_records = latest_dict.get("invalid_records")
        failure_rate = latest_dict.get("failure_rate")

        logger.info(
            "Latest Silver validation metrics read: "
            f"run_id={pipeline_run_id}, "
            f"total_records={total_records}, "
            f"valid_records={valid_records}, "
            f"invalid_records={invalid_records}, "
            f"failure_rate={failure_rate:.6f}"
        )

        if failure_rate is None:
            raise RuntimeError(
                "Latest validation metrics record does not contain failure_rate. "
                "Verify Silver metrics schema and re-run the pipeline."
            )

        if failure_rate > threshold:
            logger.error(
                "DATA QUALITY THRESHOLD BREACHED: "
                f"failure_rate={failure_rate:.6f} exceeds threshold={threshold:.6f}"
            )
            raise RuntimeError(
                "Data Quality check failed because the silver failure_rate "
                "exceeded the configured threshold."
            )

        logger.info("Data Quality Check passed. No threshold breach detected.")

    except Exception as e:
        logger.error(f"DQ Check failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
