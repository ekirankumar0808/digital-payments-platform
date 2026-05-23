from pyspark.sql import SparkSession
import os


def create_spark_session(app_name: str) -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(app_name)

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # S3
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID")
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.ap-south-1.amazonaws.com"
        )

        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")

        .getOrCreate()
    )

    return spark