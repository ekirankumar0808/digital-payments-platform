import pytest
import os

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestSession")
        
        # -----------------------------
        # Performance & stability
        # -----------------------------
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")

        # -----------------------------
        # Avoid local warehouse issues
        # -----------------------------
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

        # -----------------------------
        # Disable Hive (CI stability)
        # -----------------------------
        .config("spark.sql.catalogImplementation", "in-memory")

        # -----------------------------
        # Deterministic behaviour
        # -----------------------------
        .config("spark.sql.session.timeZone", "UTC")

        .getOrCreate()
    )

    yield spark

    spark.stop()