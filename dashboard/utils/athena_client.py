import awswrangler as wr
import pandas as pd


DATABASE = "digital_payments_analytics"

ATHENA_OUTPUT = (
    "s3://digital-payments-kiran/athena-query-results/"
)


def run_query(query):

    df = wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT
    )

    return df