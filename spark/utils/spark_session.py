from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os


def create_spark_session(app_name: str) -> SparkSession:

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ----------------------------
        # Delta Lake configs
        # ----------------------------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # ----------------------------
        # S3A configs (minimal but correct)
        # ----------------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # AWS credentials (from env)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

        # ----------------------------
        # S3 tuning
        # ----------------------------
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    )

    builder = configure_spark_with_delta_pip(builder)
    builder = builder.config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.404"
        ])
    )

    spark = builder.getOrCreate()

    return spark