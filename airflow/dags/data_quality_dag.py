from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


FRAUD_DB_CONFIG = {
    "host": os.getenv("FRAUD_DB_HOST", "postgres"),
    "port": int(os.getenv("FRAUD_DB_PORT", "5432")),
    "database": os.getenv("FRAUD_DB_NAME", "fraud_detection"),
    "user": os.getenv("FRAUD_DB_USER", "fraud_user"),
    "password": os.getenv("FRAUD_DB_PASSWORD", "fraud_password"),
}

REPORTS_DIR = "/opt/airflow/reports"


QUALITY_CHECKS = {
    "missing_transaction_id": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE transaction_id IS NULL;
    """,

    "missing_user_id": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE user_id IS NULL;
    """,

    "invalid_amount": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE amount IS NULL OR amount <= 0;
    """,

    "invalid_transaction_status": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE transaction_status NOT IN ('NORMAL', 'SUSPICIOUS', 'FRAUD')
        OR transaction_status IS NULL;
    """,

    "invalid_risk_score": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE risk_score IS NULL OR risk_score < 0 OR risk_score > 100;
    """,

    "missing_transaction_timestamp": """
        SELECT COUNT(*) AS issue_count
        FROM transactions
        WHERE transaction_timestamp IS NULL;
    """,

    "duplicated_transaction_id": """
        SELECT COUNT(*) AS issue_count
        FROM (
            SELECT transaction_id
            FROM transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "fraud_alert_without_transaction": """
        SELECT COUNT(*) AS issue_count
        FROM fraud_alerts fa
        LEFT JOIN transactions t
        ON fa.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL;
    """,

    "fraud_transaction_without_alert": """
        SELECT COUNT(*) AS issue_count
        FROM transactions t
        LEFT JOIN fraud_alerts fa
        ON t.transaction_id = fa.transaction_id
        WHERE t.transaction_status = 'FRAUD'
        AND fa.transaction_id IS NULL;
    """
}


def run_data_quality_checks():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    conn = psycopg2.connect(**FRAUD_DB_CONFIG)

    results = []

    for check_name, query in QUALITY_CHECKS.items():
        df = pd.read_sql_query(query, conn)
        issue_count = int(df["issue_count"].iloc[0])

        status = "PASSED" if issue_count == 0 else "FAILED"

        results.append({
            "check_name": check_name,
            "status": status,
            "issue_count": issue_count,
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    conn.close()

    result_df = pd.DataFrame(results)

    report_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = os.path.join(REPORTS_DIR, f"data_quality_report_{report_date}.csv")

    result_df.to_csv(report_path, index=False)

    print("Data quality report generated:", report_path)
    print(result_df)

    failed_checks = result_df[result_df["status"] == "FAILED"]

    if not failed_checks.empty:
        print("Some data quality checks failed.")
        print(failed_checks)
        raise ValueError("Data quality checks failed. Please inspect the generated report.")
    else:
        print("All data quality checks passed.")


default_args = {
    "owner": "fraud-detection-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="Run data quality checks on fraud detection PostgreSQL tables",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "data-quality", "postgresql"],
) as dag:

    run_quality_checks_task = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    run_quality_checks_task