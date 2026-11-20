import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

loger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}

def analyze_fraud_alerts():
    hook = PostgresHook(postgres_conn_id='postgres_fraud')

    summary_sql = """
        SELECT
            COUNT(*) as total_alerts,
            COALESCE(SUM(amount), 0) as total_fraud_amount,
            COALESCE(AVG(amount), 0) as avg_fraud_amount
        FROM fraud_alerts;
    """
    total_alerts, total_amount, avg_amount = hook.get_first(summary_sql)

    loger.info("Fraud Alerts Summary: total_alerts=%s, total_volume=%.2f, avg_amount=%.2f", total_alerts, float(total_amount), float(avg_amount))

    top_categories_sql = """
        SELECT merchant_category, COUNT(*) as alert_count, SUM(amount) as category_sum
        FROM fraud_alerts
        GROUP BY merchant_category
        ORDER BY alert_count DESC;
    """

    records = hook.get_records(top_categories_sql)
    for category, count, sum_amount in records:
        loger.info("Category: %s | Alerts: %s | Volume: %.2f",
                   category, count, float(sum_amount))

with DAG(
    dag_id='fraud_alerts_monitor_dag',
    default_args=default_args,
    description='Regular audit and metrics aggregation for fraud_alerts table',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['fraud', 'monitoring', 'postgres'],
) as dag:

    audit_task = PythonOperator(
        task_id='audit_fraud_alerts.task',
        python_callable=analyze_fraud_alerts
    )