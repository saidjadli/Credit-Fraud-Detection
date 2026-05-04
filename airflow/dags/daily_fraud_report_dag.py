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
            COALESCE(SUM(amount) FILTER (WHERE transaction_status = 'FRAUD'), 0) AS total_fraud_amount,
            COALESCE(AVG(ml_probability), 0) AS avg_ml_probability,
            COALESCE(MAX(ml_probability), 0) AS max_ml_probability,
            COALESCE(AVG(behavior_score), 0) AS avg_behavior_score,
            COALESCE(AVG(final_score), 0) AS avg_final_score,
            COALESCE(MAX(final_score), 0) AS max_final_score
        FROM transactions;
    """

    query_status_distribution = """
        SELECT
            transaction_status,
            COUNT(*) AS total,
            COALESCE(AVG(ml_probability), 0) AS avg_ml_probability,
            COALESCE(AVG(final_score), 0) AS avg_final_score,
            COALESCE(MAX(final_score), 0) AS max_final_score
        FROM transactions
        GROUP BY transaction_status
        ORDER BY transaction_status;
    """

    query_fraud_details = """
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
            ml_probability,
            behavior_score,
            final_score,
            transaction_status,
            actual_label,
            detection_method,
            transaction_timestamp,
            processed_at
        FROM transactions
        WHERE transaction_status IN ('SUSPICIOUS', 'FRAUD')
        ORDER BY processed_at DESC;
    """

    query_top_countries = """
        SELECT
            country,
            COUNT(*) AS total_alerts,
            COUNT(*) FILTER (WHERE transaction_status = 'FRAUD') AS total_fraud,
            COUNT(*) FILTER (WHERE transaction_status = 'SUSPICIOUS') AS total_suspicious,
            COALESCE(SUM(amount) FILTER (WHERE transaction_status = 'FRAUD'), 0) AS fraud_amount
        FROM transactions
        WHERE transaction_status IN ('SUSPICIOUS', 'FRAUD')
        GROUP BY country
        ORDER BY total_alerts DESC
        LIMIT 20;
    """

    summary_df = pd.read_sql_query(query_summary, conn)
    status_distribution_df = pd.read_sql_query(query_status_distribution, conn)
    fraud_details_df = pd.read_sql_query(query_fraud_details, conn)
    top_countries_df = pd.read_sql_query(query_top_countries, conn)

    conn.close()

    report_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    summary_path = os.path.join(REPORTS_DIR, f"fraud_summary_{report_date}.csv")
    status_path = os.path.join(REPORTS_DIR, f"fraud_status_distribution_{report_date}.csv")
    details_path = os.path.join(REPORTS_DIR, f"fraud_details_{report_date}.csv")
    countries_path = os.path.join(REPORTS_DIR, f"fraud_top_countries_{report_date}.csv")

    summary_df.to_csv(summary_path, index=False)
    status_distribution_df.to_csv(status_path, index=False)
    fraud_details_df.to_csv(details_path, index=False)
    top_countries_df.to_csv(countries_path, index=False)

    print("Summary report generated:", summary_path)
    print("Status distribution report generated:", status_path)
    print("Fraud details report generated:", details_path)
    print("Top countries report generated:", countries_path)


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
    tags=["fraud", "reporting", "postgresql", "ml"],
) as dag:

    generate_report_task = PythonOperator(
        task_id="generate_daily_fraud_report",
        python_callable=generate_daily_fraud_report,
    )

    generate_report_task