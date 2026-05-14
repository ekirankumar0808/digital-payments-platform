from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


transaction_schema = StructType([

    StructField(
        "transaction_id",
        StringType(),
        True
    ),

    StructField(
        "merchant",
        StringType(),
        True
    ),

    StructField(
        "amount",
        DoubleType(),
        True
    ),

    StructField(
        "status",
        StringType(),
        True
    ),

    StructField(
        "transaction_date",
        StringType(),
        True
    ),

    StructField(
        "currency",
        StringType(),
        True
    )
])