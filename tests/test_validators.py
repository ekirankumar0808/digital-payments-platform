from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from spark.utils.validators import validate_transaction_type


def test_validate_transaction_type(spark_session):

    input_data = [
        (
            1,
            "PAYMENT",
            9839.64,
            "C1231006815",
            170136.0,
            160296.36,
            "M1979787155",
            0.0,
            0.0,
            0,
            0,
            "INVALID_TRANSACTION_TYPE"
        ),
        (
            1,
            "TRANSFER",
            181.0,
            "C1305486145",
            181.0,
            0.0,
            "C553264065",
            0.0,
            0.0,
            1,
            0,
            "INVALID_TRANSACTION_TYPE"
        ),
        (
            1,
            "HACK",
            5000.0,
            "C999999999",
            10000.0,
            5000.0,
            "C888888888",
            0.0,
            5000.0,
            0,
            0,
            "INVALID_TRANSACTION_TYPE"
        )  # invalid transaction type
    ]

    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True),
        StructField("transaction_type", StringType(), True)
    ])

    input_df = spark_session.createDataFrame(input_data, schema)

    actual_df = validate_transaction_type(input_df)

    expected_data = [
        (
            1,
            "HACK",
            5000.0,
            "C999999999",
            10000.0,
            5000.0,
            "C888888888",
            0.0,
            5000.0,
            0,
            0,
            "INVALID_TRANSACTION_TYPE"
        )
    ]

    expected_df = spark_session.createDataFrame(expected_data, schema)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True
    )