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


def safe_divide(numerator, denominator):
    if denominator == 0:
        return 0.0
    return numerator / denominator


def generate_model_evaluation_report():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    conn = psycopg2.connect(**FRAUD_DB_CONFIG)

    confusion_query = """
        SELECT
            SUM(CASE WHEN actual_label = 1 AND transaction_status IN ('SUSPICIOUS', 'FRAUD') THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN actual_label = 0 AND transaction_status IN ('SUSPICIOUS', 'FRAUD') THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN actual_label = 0 AND transaction_status = 'NORMAL' THEN 1 ELSE 0 END) AS true_negatives,
            SUM(CASE WHEN actual_label = 1 AND transaction_status = 'NORMAL' THEN 1 ELSE 0 END) AS false_negatives
        FROM transactions
        WHERE actual_label IS NOT NULL;
    """

    distribution_query = """
        SELECT
            actual_label,
            transaction_status,
            COUNT(*) AS total,
            COALESCE(AVG(ml_probability), 0) AS avg_ml_probability,
            COALESCE(AVG(behavior_score), 0) AS avg_behavior_score,
            COALESCE(AVG(final_score), 0) AS avg_final_score,
            COALESCE(MAX(final_score), 0) AS max_final_score
        FROM transactions
        WHERE actual_label IS NOT NULL
        GROUP BY actual_label, transaction_status
        ORDER BY actual_label, transaction_status;
    """

    top_predictions_query = """
        SELECT
            transaction_id,
            user_id,
            amount,
            country,
            usual_country,
            merchant_category,
            payment_method,
            ml_probability,
            behavior_score,
            final_score,
            risk_score,
            transaction_status,
            actual_label,
            detection_method,
            transaction_timestamp,
            processed_at
        FROM transactions
        WHERE actual_label IS NOT NULL
        ORDER BY final_score DESC
        LIMIT 100;
    """

    confusion_df = pd.read_sql_query(confusion_query, conn)
    distribution_df = pd.read_sql_query(distribution_query, conn)
    top_predictions_df = pd.read_sql_query(top_predictions_query, conn)

    conn.close()

    tp = int(confusion_df["true_positives"].fillna(0).iloc[0])
    fp = int(confusion_df["false_positives"].fillna(0).iloc[0])
    tn = int(confusion_df["true_negatives"].fillna(0).iloc[0])
    fn = int(confusion_df["false_negatives"].fillna(0).iloc[0])

    precision = safe_divide(tp, tp + fp)
    recall = safe_divide(tp, tp + fn)
    f1_score = safe_divide(2 * precision * recall, precision + recall)
    accuracy = safe_divide(tp + tn, tp + fp + tn + fn)

    metrics_df = pd.DataFrame([{
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1_score, 4),
        "accuracy": round(accuracy, 4),
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])

    report_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    metrics_path = os.path.join(REPORTS_DIR, f"model_evaluation_metrics_{report_date}.csv")
    distribution_path = os.path.join(REPORTS_DIR, f"model_evaluation_distribution_{report_date}.csv")
    top_predictions_path = os.path.join(REPORTS_DIR, f"model_top_predictions_{report_date}.csv")

    metrics_df.to_csv(metrics_path, index=False)
    distribution_df.to_csv(distribution_path, index=False)
    top_predictions_df.to_csv(top_predictions_path, index=False)

    print("Model evaluation metrics generated:", metrics_path)
    print("Model evaluation distribution generated:", distribution_path)
    print("Top predictions report generated:", top_predictions_path)
    print(metrics_df)


default_args = {
    "owner": "fraud-detection-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="model_evaluation_report_dag",
    default_args=default_args,
    description="Generate ML and hybrid scoring evaluation reports",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "ml", "evaluation", "reporting"],
) as dag:

    generate_model_evaluation_task = PythonOperator(
        task_id="generate_model_evaluation_report",
        python_callable=generate_model_evaluation_report,
    )

    generate_model_evaluation_task