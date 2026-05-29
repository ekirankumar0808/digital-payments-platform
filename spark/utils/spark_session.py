from pyspark.sql import SparkSession


def create_spark_session(app_name: str):

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

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )

        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")

        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark