from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from spark.utils.validators import validate_transaction_type


def test_validate_transaction_type(spark_session):

    input_data = [
        (1, "PAYMENT", 100.0, "A", "B"),
        (2, "TRANSFER", 200.0, "C", "D"),
        (3, "HACK", 300.0, "E", "F")  # invalid
    ]

    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("nameDest", StringType(), True)
    ])

    input_df = spark_session.createDataFrame(input_data, schema)

    actual_df = validate_transaction_type(input_df)

    expected_data = [
        (3, "HACK", 300.0, "E", "F")
    ]

    expected_df = spark_session.createDataFrame(expected_data, schema)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True
    )