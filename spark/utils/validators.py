from pyspark.sql.functions import col, lit

# ------------------------------------------------------
# 1. VALIDATE POSITIVE AMOUNT
# ------------------------------------------------------
def validate_positive_amount(df):

    invalid_df = (
        df.filter(
            col("amount") <= 0
        )
        .withColumn(
            "validation_reason",
            lit("INVALID_AMOUNT")
        )
    )

    return invalid_df


# ------------------------------------------------------
# 2. VALIDATE NULL CRITICAL FIELDS (PAYSIM FIXED)
# ------------------------------------------------------
def validate_null_transaction_id(df):
    """
    PaySim does NOT have transaction_id.
    We validate business keys instead:
    step, nameOrig, nameDest
    """

    invalid_df = df.filter(
        col("step").isNull() |
        col("nameOrig").isNull() |
        col("nameDest").isNull()
    )

    invalid_df = invalid_df.withColumn(
        "validation_reason",
        lit("MISSING_REQUIRED_FIELDS")
    )

    return invalid_df


# ------------------------------------------------------
# 3. VALIDATE BALANCE CONSISTENCY (FINANCIAL SANITY)
# ------------------------------------------------------
def validate_balance(df):
    """
    Detect negative or inconsistent balances
    """

    invalid_df = df.filter(
        (col("oldbalanceOrg") < 0) |
        (col("newbalanceOrig") < 0) |
        (col("oldbalanceDest") < 0) |
        (col("newbalanceDest") < 0)
    ).withColumn("validation_reason", lit("NEGATIVE_OR_INVALID_BALANCE"))

    return invalid_df


# ------------------------------------------------------
# 4. VALIDATE TRANSACTION TYPE (PAYSIM DOMAIN RULE)
# ------------------------------------------------------
def validate_transaction_type(df):
    """
    Only valid PaySim types allowed
    """

    valid_types = [
        "PAYMENT",
        "TRANSFER",
        "CASH_OUT",
        "DEBIT",
        "CASH_IN"
    ]

    invalid_df = (
        df.filter(
            ~col("type").isin(valid_types)
        )
        .withColumn(
            "validation_reason",
            lit("INVALID_TRANSACTION_TYPE")
        )
    )

    return invalid_df