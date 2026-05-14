from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

from spark.utils.validators import validate_currency


def test_validate_currency(spark_session):

    input_data = [
        (1, "INR"),
        (2, "USD"),
        (3, "ABC")
    ]

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("currency", StringType(), True)
    ])

    input_df = spark_session.createDataFrame(
        input_data,
        schema
    )

    actual_df = validate_currency(input_df)

    expected_data = [
        (3, "ABC")
    ]

    expected_df = spark_session.createDataFrame(
        expected_data,
        schema
    )

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True
    )