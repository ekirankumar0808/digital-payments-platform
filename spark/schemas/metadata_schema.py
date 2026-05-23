from pyspark.sql.types import *

bronze_metadata_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("modification_time", LongType(), True),
    StructField("row_count", LongType(), True),
    StructField("status", StringType(), True),  # SUCCESS / FAILED
    StructField("ingestion_timestamp", TimestampType(), True)
])