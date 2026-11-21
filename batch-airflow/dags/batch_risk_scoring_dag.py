import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def check_source_data_availability():
    # Проверка наличие свежих алертов в фрод алертс перед запуском спарк
    logger.info("Executing Pre-check: validating fraud_alerts source table...")
    hook = PostgresHook(postgres_conn_id="postgres_fraud")
    records = hook.get_records("SELECT COUNT(*) FROM fraud_alerts;")
    count = records[0][0] if records else 0

    if count == 0:
        raise ValueError("Data Pre-check failed: table fraud_alerts is empty. Aborting batch run.")

    logger.info(f"Data Pre-Check passed: found {count} alerts available for processing.")

def validate_data_quality():
    logger.info("Executing Data Quality Gate on merchant_risk_metrics...")
    hook = PostgresHook(postgres_conn_id="postgres_fraud")

    query = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(CASE WHEN risk_score < 0 OR risk_score > 100 THEN 1 END) as invalid_scores,
            COUNT(CASE WHEN risk_level IS NULL THEN 1 END) as null_levels
        FROM merchant_risk_metrics;
    """

    row = hook.get_first(query)
    total_rows, invalid_scores, null_levels = row

    if total_rows == 0:
        raise AssertionError("Data Quality check failed: merchant_risk_metrics is empty.")
    if invalid_scores > 0:
        raise AssertionError(f"Data Quality check failed: found {invalid_scores} rows with invalid risk_score.")
    if null_levels > 0:
        raise AssertionError(f"Data Quality check failed: found {null_levels} rows with NULL risk_level.")
    logger.info(
        f"Data Quality Gate PASSED: {total_rows} merchant risk profiles verified successfully."
    )

with DAG(
    dag_id="batch_risk_scoring_dag",
    default_args=default_args,
    description="Daily batch pipeline: PySpark Merchant Risk Scoring & Data Quality Gate",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 20),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "pyspark", "risk_scoring", "postgres"],
) as dag:

    task_check_source = PythonOperator(
        task_id="check_source_alerts",
        python_callable=check_source_data_availability,
    )

    task_run_pyspark = BashOperator(
        task_id="run_pyspark_scoring",
        bash_command="python /opt/airflow/batch-analytics/scripts/merchant_risk_scoring.py",
    )

    task_quality_gate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    task_check_source >> task_run_pyspark >> task_quality_gate