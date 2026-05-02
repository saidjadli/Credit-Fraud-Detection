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


def repair_missing_alerts():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    conn = psycopg2.connect(**FRAUD_DB_CONFIG)
    conn.autocommit = False

    try:
        query_missing_alerts = """
            SELECT
                t.transaction_id,
                t.user_id,
                t.amount,
                t.currency,
                t.country,
                t.usual_country,
                t.merchant_category,
                t.payment_method,
                t.risk_score,
                t.transaction_status,
                t.transaction_timestamp,
                t.processed_at
            FROM transactions t
            LEFT JOIN fraud_alerts fa
            ON t.transaction_id = fa.transaction_id
            WHERE t.transaction_status IN ('SUSPICIOUS', 'FRAUD')
            AND fa.transaction_id IS NULL
            ORDER BY t.processed_at DESC;
        """

        missing_df = pd.read_sql_query(query_missing_alerts, conn)

        insert_query = """
            INSERT INTO fraud_alerts (
                transaction_id,
                user_id,
                risk_score,
                alert_type,
                message,
                status,
                created_at
            )
            SELECT
                t.transaction_id,
                t.user_id,
                t.risk_score,
                'REAL_TIME_FRAUD_DETECTION',
                'Transaction ' || t.transaction_id ||
                ' detected as ' || t.transaction_status ||
                ' with risk score ' || t.risk_score,
                'OPEN',
                CURRENT_TIMESTAMP
            FROM transactions t
            LEFT JOIN fraud_alerts fa
            ON t.transaction_id = fa.transaction_id
            WHERE t.transaction_status IN ('SUSPICIOUS', 'FRAUD')
            AND fa.transaction_id IS NULL;
        """

        cursor = conn.cursor()
        cursor.execute(insert_query)
        repaired_count = cursor.rowcount
        conn.commit()
        cursor.close()

        report_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        report_path = os.path.join(REPORTS_DIR, f"repair_missing_alerts_{report_date}.csv")

        if missing_df.empty:
            report_df = pd.DataFrame([{
                "status": "NO_MISSING_ALERTS",
                "repaired_count": 0,
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }])
        else:
            missing_df["repair_status"] = "REPAIRED"
            missing_df["repaired_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            missing_df["repaired_count_total"] = repaired_count
            report_df = missing_df

        report_df.to_csv(report_path, index=False)

        print(f"Missing alerts repaired: {repaired_count}")
        print(f"Repair report generated: {report_path}")

    except Exception as e:
        conn.rollback()
        print("Repair failed. Transaction rolled back.")
        raise e

    finally:
        conn.close()


default_args = {
    "owner": "fraud-detection-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="repair_missing_alerts_dag",
    default_args=default_args,
    description="Repair missing fraud alerts for suspicious and fraudulent transactions",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fraud", "repair", "data-quality", "postgresql"],
) as dag:

    repair_missing_alerts_task = PythonOperator(
        task_id="repair_missing_alerts",
        python_callable=repair_missing_alerts,
    )

    repair_missing_alerts_task