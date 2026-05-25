import awswrangler as wr
import boto3
import os


def run_query(query):

    session = boto3.Session(
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    df = wr.athena.read_sql_query(
        sql=query,
        database="your_database_name",
        boto3_session=session
    )

    return df