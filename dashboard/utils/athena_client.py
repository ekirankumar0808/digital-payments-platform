import awswrangler as wr
import boto3
import os


def run_query(query):

    session = boto3.Session(
        region_name=os.getenv("AWS_REGION", "ap-south-1")
    )

    df = wr.athena.read_sql_query(
        sql=query,
        database="digital_payments_analytics",
        boto3_session=session
    )

    return df