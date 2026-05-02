from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


FRAUD_DB_CONFIG = {
    "host": os.getenv("FRAUD_DB_HOST", "fraud-postgres"),
    "port": int(os.getenv("FRAUD_DB_PORT", "5432")),
    "database": os.getenv("FRAUD_DB_NAME", "fraud_detection"),
    "user": os.getenv("FRAUD_DB_USER", "fraud_user"),
    "password": os.getenv("FRAUD_DB_PASSWORD", "fraud_password"),
}

REPORTS_DIR = "/opt/airflow/reports"


def generate_daily_fraud_report():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    conn = psycopg2.connect(**FRAUD_DB_CONFIG)

    query_summary = """
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(*) FILTER (WHERE transaction_status = 'NORMAL') AS total_normal,
            COUNT(*) FILTER (WHERE transaction_status = 'SUSPICIOUS') AS total_suspicious,
            COUNT(*) FILTER (WHERE transaction_status = 'FRAUD') AS total_fraud,
            COALESCE(SUM(amount) FILTER (WHERE transaction_status = 'FRAUD'), 0) AS total_fraud_amount
        FROM transactions;
    """

    query_frauds = """
        SELECT
            transaction_id,
            user_id,
            amount,
            currency,
            country,
            usual_country,
            merchant_category,
            payment_method,
            risk_score,
            transaction_status,
            transaction_timestamp,
            processed_at
        FROM transactions
        WHERE transaction_status IN ('SUSPICIOUS', 'FRAUD')
        ORDER BY processed_at DESC;
    """

    summary_df = pd.read_sql_query(query_summary, conn)
    frauds_df = pd.read_sql_query(query_frauds, conn)

    conn.close()

    report_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    summary_path = os.path.join(REPORTS_DIR, f"fraud_summary_{report_date}.csv")
    details_path = os.path.join(REPORTS_DIR, f"fraud_details_{report_date}.csv")

    summary_df.to_csv(summary_path, index=False)
    frauds_df.to_csv(details_path, index=False)

    print(f"Summary report generated: {summary_path}")
    print(f"Fraud details report generated: {details_path}")


default_args = {
    "owner": "fraud-detection-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_fraud_report_dag",
    default_args=default_args,
    description="Generate daily fraud detection reports from PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "reporting", "postgresql"],
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_daily_fraud_report",
        python_callable=generate_daily_fraud_report,
    )

    generate_report_task