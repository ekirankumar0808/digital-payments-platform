from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

def write_audit_log(
    spark,
    audit_path,
    pipeline_name,
    layer,
    run_id,
    status,
    records_processed,
    error_message=None
):

    data = [(
        pipeline_name,
        layer,
        run_id,
        status,
        records_processed,
        error_message
    )]

    schema = StructType([
        StructField("pipeline_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("records_processed", LongType(), True),
        StructField("error_message", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema) \
        .withColumn("audit_timestamp", current_timestamp())

    df.write \
        .format("delta") \
        .mode("append") \
        .save(audit_path)