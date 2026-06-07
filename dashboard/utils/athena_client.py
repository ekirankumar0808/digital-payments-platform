import awswrangler as wr
import boto3
import os


def run_query(config, query):

    session = boto3.Session(
        region_name=os.getenv("AWS_REGION", "ap-south-1")
    )

    df = wr.athena.read_sql_query(
        sql=query,
        database=config["athena"]["database"],
        boto3_session=session,
        s3_output=config["athena"]["output_location"]
        ctas_approach=False

    )

    return df