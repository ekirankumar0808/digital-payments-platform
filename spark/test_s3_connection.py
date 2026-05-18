from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3ConnectionTest") \
    .getOrCreate()

df = spark.createDataFrame([
    (1, "payment"),
    (2, "refund")
], ["id", "type"])

df.show()

df.write.mode("overwrite").parquet(
    "s3a://digital-payments-kiran/test-data/"
)

print("Data written successfully to S3")


