import os
from pyspark.sql.functions import col, sum as _sum, lit, current_timestamp
from spark.utils.config_loader import load_config
from spark.utils.spark_session import create_spark_session
from spark.utils.logger import get_logger

# ------------------------------------------------------
# ENV + CONFIG
# ------------------------------------------------------
env = os.getenv("ENV", "dev")
config = load_config(env)

logger = get_logger(
    logger_name="DataQualityCheck",
    log_path=config['logging']['log_path'],
    log_level=config['logging']['log_level']
)

spark = create_spark_session("DataQualityCheck")

try:
    logger.info("Starting Data Quality Check")

    dq_path = config['paths']['dataquality']
    silver_path = config['paths']['silver']
    dq_metrics_path = config['paths'].get('dq_metrics', dq_path)

    # ------------------------------------------------------
    # READ QUARANTINE METRICS ONLY
    # ------------------------------------------------------
    dq_df = spark.read.format("delta").load(dq_path)

    total_bad = dq_df.groupBy().sum("count").collect()[0][0]

    logger.info(f"Total Bad Records: {total_bad}")

    # ------------------------------------------------------
    # SILVER TOTAL (optional monitoring reference)
    # ------------------------------------------------------
    silver_df = spark.read.format("delta").load(silver_path)
    total_records = silver_df.count()

    logger.info(f"Total Silver Records: {total_records}")

    # ------------------------------------------------------
    # FAILURE RATE
    # ------------------------------------------------------
    total_bad = 0 if total_bad is None else total_bad
    failure_rate = (total_bad / total_records) if total_records > 0 else 0

    logger.info(f"Failure Rate: {failure_rate}")

    # ------------------------------------------------------
    # STORE DQ METRICS (IMPORTANT ADDITION)
    # ------------------------------------------------------
    metrics_df = spark.createDataFrame([
        (total_bad, total_records, failure_rate)
    ], ["bad_records", "total_records", "failure_rate"]) \
    .withColumn("timestamp", current_timestamp())

    metrics_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(dq_metrics_path)

    # ------------------------------------------------------
    # THRESHOLD RULE
    # ------------------------------------------------------
    threshold = float(config.get("dq", {}).get("failure_threshold", 0.05))

    logger.info(f"Threshold: {threshold}")

    if failure_rate > threshold:
        logger.error(f"""
        🚨 DATA QUALITY ALERT
        Bad Records: {total_bad}
        Total Records: {total_records}
        Failure Rate: {failure_rate}
        Threshold: {threshold}
        """)

        raise Exception("DATA QUALITY THRESHOLD BREACHED")

    # ------------------------------------------------------
    # SUCCESS
    # ------------------------------------------------------
    logger.info("✅ Data Quality Check Passed")

    spark.stop()

except Exception as e:
    logger.error(f"DQ Check Failed: {str(e)}")
    spark.stop()
    raise e