from pyspark.sql.functions import col

def validate_positive_amount(df):
    return df.filter(col("amount") <= 0)

def validate_null_transaction_id(df):
    return df.filter(col("transaction_id").isNull())

def validate_currency(df):
    valid_currencies = ["INR", "USD", "EUR"]
    return df.filter(~col("currency").isin(valid_currencies))