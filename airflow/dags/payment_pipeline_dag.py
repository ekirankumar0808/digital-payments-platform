from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
from airflow.utils.email import send_email

# ----------------------------
# EMAIL CONFIG
# ----------------------------
EMAIL_RECIPIENTS = ["ekirankumar0808@gmail.com"]

# ----------------------------
# Default Args (UPDATED)
# ----------------------------
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,

    # ----------------------------
    # RETRIES
    # ----------------------------
    "retries": 3,
    "retry_delay": timedelta(minutes=5),

    # ----------------------------
    # EMAIL ALERTS (IMPORTANT)
    # ----------------------------
    "email": EMAIL_RECIPIENTS,
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,   # 🔥 success alert enabled
}


# ----------------------------
# FAILURE CALLBACK (extra logging)
# ----------------------------
def task_failure_alert(context):
    task_instance = context.get("task_instance")
    logging.error(f"""
    ❌ TASK FAILED
    DAG: {task_instance.dag_id}
    Task: {task_instance.task_id}
    Run: {task_instance.run_id}
    """)

# ----------------------------
# SUCCESS CALLBACK (optional)
# ----------------------------
def task_success_alert(context):
    task_instance = context.get("task_instance")

    subject = f"✅ SUCCESS: {task_instance.task_id}"

    html_content = f"""
    <h3>Task Success</h3>

    <p><b>DAG:</b> {task_instance.dag_id}</p>
    <p><b>Task:</b> {task_instance.task_id}</p>
    <p><b>Run ID:</b> {task_instance.run_id}</p>
    """

    send_email(
        to=EMAIL_RECIPIENTS,
        subject=subject,
        html_content=html_content
    )

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="digital_payments_pipeline",
    description="Production-grade Delta Lakehouse ETL Pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    on_failure_callback=task_failure_alert,
    tags=["data-engineering", "spark", "delta-lake", "aws"]
) as dag:

    # ----------------------------
    # BRONZE
    # ----------------------------
    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        set -euo pipefail

        echo "🚀 Starting Bronze Ingestion"

        docker exec spark-delta-engine python /app/spark/bronze/bronze_ingestion.py
        """,
        retries=3,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=30),
        email=EMAIL_RECIPIENTS,
        email_on_failure=True,
        email_on_retry=False,
        on_success_callback=task_success_alert  # 🔥 success alert for bronze
    )

    bronze_task.doc_md = """
        ### Bronze Layer

        - Reads raw payment transactions
        - Loads raw parquet data from S3
        - Writes Delta Bronze tables
        
        """

    # ----------------------------
    # SILVER
    # ----------------------------
    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        set -euo pipefail

        echo "🚀 Starting Silver Transformation"

        docker exec spark-delta-engine python /app/spark/silver/silver_transformation.py
        """,
        retries=3,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=30),
        email=EMAIL_RECIPIENTS,
        email_on_failure=True,
        on_success_callback=task_success_alert  # 🔥 success alert for silver
    )

    silver_task.doc_md = """
        ### Silver Layer

        - Transforms raw payment data
        - Applies business logic
        - Writes Delta Silver tables
        """

    # ----------------------------
    # DQ CHECK
    # ----------------------------
    dq_check_task = BashOperator(
        task_id="data_quality_check",
        bash_command="""
        set -euo pipefail

        echo "🔍 Running Data Quality Check"

        docker exec spark-delta-engine python /app/spark/utils/dq_check.py
        """,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
        email=EMAIL_RECIPIENTS,
        email_on_failure=True,
        on_success_callback=task_success_alert  # 🔥 success alert for data quality check
    )

    dq_check_task.doc_md = """
        ### Data Quality Check

        - Validates data quality metrics
        - Checks for anomalies and inconsistencies
        - Sends alerts for failed checks
        """

    # ----------------------------
    # GOLD
    # ----------------------------
    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        set -euo pipefail

        echo "🚀 Starting Gold Aggregation"

        docker exec spark-delta-engine python /app/spark/gold/gold_aggregation.py
        """,
        retries=3,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=30),
        email=EMAIL_RECIPIENTS,
        email_on_failure=True,
        on_success_callback=task_success_alert  # 🔥 success alert for gold
    )

    gold_task.doc_md = """
        ### Gold Layer
        - Aggregates silver data for analytics
        - Writes Delta Gold tables
        """


    # ----------------------------
    # FLOW
    # ----------------------------
    bronze_task >> silver_task >> dq_check_task >> gold_task