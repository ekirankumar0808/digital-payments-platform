from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# ---------------------------------------------------------
# EMAIL CONFIG
# ---------------------------------------------------------
EMAIL_RECIPIENTS = ["ekirankumar0808@gmail.com"]

# ---------------------------------------------------------
# DEFAULT ARGS
# ---------------------------------------------------------
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": EMAIL_RECIPIENTS,
    "email_on_failure": True,
    "email_on_retry": False,
}

# ---------------------------------------------------------
# CALLBACKS
# ---------------------------------------------------------
def task_failure_alert(context):
    ti = context.get("task_instance")
    logging.error(f"❌ FAILED: {ti.dag_id} | {ti.task_id} | {ti.run_id}")

def task_success_alert(context):
    ti = context.get("task_instance")
    logging.info(f"✅ SUCCESS: {ti.dag_id} | {ti.task_id} | {ti.run_id}")

# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------
with DAG(
    dag_id="digital_payments_pipeline",
    description="Production-grade Delta Lakehouse ETL Pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    concurrency=4,
    on_failure_callback=task_failure_alert,
    tags=["data-engineering", "spark", "delta-lake", "aws", "lakehouse"]
) as dag:

    # -----------------------------------------------------
    # BRONZE
    # -----------------------------------------------------
    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        set -e
        echo "🚀 Starting Bronze Ingestion"

        docker exec spark-delta-engine spark-submit /app/spark/bronze/bronze_ingestion.py \
        2>&1 | tee /tmp/bronze.log
        """,
        on_success_callback=task_success_alert,
    )

    # -----------------------------------------------------
    # SILVER
    # -----------------------------------------------------
    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        set -e
        echo "🚀 Starting Silver Transformation"

        docker exec spark-delta-engine spark-submit /app/spark/silver/silver_transformation.py \
        2>&1 | tee /tmp/silver.log
        """,
        on_success_callback=task_success_alert,
    )

    # -----------------------------------------------------
    # DATA QUALITY CHECK
    # -----------------------------------------------------
    dq_task = BashOperator(
        task_id="data_quality_check",
        bash_command="""
        set -e
        echo "🔍 Running Data Quality Check"

        docker exec spark-delta-engine spark-submit /app/spark/utils/dq_check.py \
        2>&1 | tee /tmp/dq.log
        """,
        on_success_callback=task_success_alert,
    )

    # -----------------------------------------------------
    # GOLD
    # -----------------------------------------------------
    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        set -e
        echo "🚀 Starting Gold Aggregation"

        docker exec spark-delta-engine spark-submit /app/spark/gold/gold_aggregation.py \
        2>&1 | tee /tmp/gold.log
        """,
        on_success_callback=task_success_alert,
    )

    # -----------------------------------------------------
    # PIPELINE FLOW
    # -----------------------------------------------------
    bronze_task >> silver_task >> dq_task >> gold_task