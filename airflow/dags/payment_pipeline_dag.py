from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="digital_payments_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        docker exec spark-delta-engine spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /app/spark/bronze/bronze_ingestion.py
        """
    )

    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        docker exec spark-delta-engine spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /app/spark/silver/silver_transformation.py
        """
    )

    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        docker exec spark-delta-engine spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        /app/spark/gold/gold_aggregation.py
        """
    )

    bronze_task >> silver_task >> gold_task