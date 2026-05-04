# Demo Scenario

This scenario demonstrates the full workflow of the real-time fraud detection platform.

## Objective

Demonstrate that a transaction sent to Kafka is processed by Spark, scored by the hybrid fraud detection engine, stored in PostgreSQL, displayed in the dashboard, and made available for Airflow reporting and auditing.

## Prerequisites

- Docker Compose is running.
- Kafka topic `transactions_raw` has been created.
- The ML model has been trained and saved in `ml/models/fraud_rf_pipeline`.
- The dataset is available at `data/raw/creditcard.csv`.
- Python dependencies are installed for `producer/` and `dashboard/`.

## Step 1 - Start the Services

```bash
docker compose up -d --build
```

Check the main interfaces:

- Kafka UI: http://localhost:8085
- Spark Master: http://localhost:18081
- Airflow: http://localhost:8088
- PgAdmin: http://localhost:5050

## Step 2 - Create the Kafka Topic

```bash
docker exec -it fraud-kafka kafka-topics \
  --bootstrap-server fraud-kafka:29092 \
  --create \
  --if-not-exists \
  --topic transactions_raw \
  --partitions 1 \
  --replication-factor 1
```

In Kafka UI, verify that the `transactions_raw` topic exists.

## Step 3 - Train the ML Model

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  /opt/spark/ml/train_model.py
```

Verify that the following files or folders exist:

- `ml/models/fraud_rf_pipeline`
- `ml/models/metrics.json`

## Step 4 - Start Spark Streaming

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" \
  /opt/spark/fraud_app/jobs/fraud_streaming_job.py
```

In the Spark UI, verify that the streaming application is active.

## Step 5 - Send Transactions

```bash
python producer/dataset_replay_producer.py
```

The producer sends both normal and fraudulent transactions to Kafka. The fraud ratio can be configured using the `FRAUD_REPLAY_RATIO` environment variable.

In Kafka UI, open the `transactions_raw` topic and inspect the JSON messages.

## Step 6 - Visualize the Pipeline in the Dashboard

```bash
streamlit run dashboard/app.py
```

Open:

```text
http://localhost:8501
```

Key points to show:

- total number of transactions;
- `NORMAL`, `SUSPICIOUS`, and `FRAUD` transactions;
- total fraudulent amount;
- `final_score` histogram;
- `ml_probability` histogram;
- `Model Evaluation` section;
- latest processed transactions;
- latest fraud alerts.

## Step 7 - Verify PostgreSQL

Using PgAdmin or SQL, verify the main tables:

```sql
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM risk_scores;
SELECT COUNT(*) FROM fraud_alerts;
```

Check the latest transactions:

```sql
SELECT
    transaction_id,
    amount,
    country,
    usual_country,
    ml_probability,
    behavior_score,
    final_score,
    transaction_status,
    actual_label
FROM transactions
ORDER BY processed_at DESC
LIMIT 20;
```

## Step 8 - Run the Airflow DAGs

In Airflow, trigger the DAGs in the following order:

```text
repair_missing_alerts_dag
data_quality_dag
daily_fraud_report_dag
model_evaluation_report_dag
```

Check the generated exports in:

```text
data/reports/
```

Expected files:

- `repair_missing_alerts_*.csv`
- `data_quality_report_*.csv`
- `fraud_summary_*.csv`
- `fraud_status_distribution_*.csv`
- `fraud_details_*.csv`
- `fraud_top_countries_*.csv`
- `model_evaluation_metrics_*.csv`
- `model_evaluation_distribution_*.csv`
- `model_top_predictions_*.csv`

## Step 9 - Demo Conclusion

The demonstration shows a complete real-time fraud detection chain:

```text
Producer -> Kafka -> Spark Streaming -> Hybrid Scoring -> PostgreSQL -> Dashboard + Airflow Reports
```

Main strengths to highlight:

- real-time processing;
- explainable hybrid scoring;
- Machine Learning integration;
- structured PostgreSQL storage;
- interactive dashboard;
- Airflow orchestration;
- CSV exports for audit and reporting.
