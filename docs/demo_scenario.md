# Scenario de demonstration

Ce scenario montre le fonctionnement complet de la plateforme de detection de fraude en temps reel.

## Objectif

Demontrer qu'une transaction envoyee dans Kafka est traitee par Spark, scoree par le moteur hybride, stockee dans PostgreSQL, visible dans le dashboard et exploitable par Airflow.

## Prerequis

- Docker Compose lance.
- Topic Kafka `transactions_raw` cree.
- Modele ML entraine dans `ml/models/fraud_rf_pipeline`.
- Dataset disponible dans `data/raw/creditcard.csv`.
- Dependances Python installees pour `producer/` et `dashboard/`.

## Etape 1 - Lancer les services

```bash
docker compose up -d --build
```

Verifier les interfaces :

- Kafka UI : http://localhost:8085
- Spark Master : http://localhost:18081
- Airflow : http://localhost:8088
- PgAdmin : http://localhost:5050

## Etape 2 - Creer le topic Kafka

```bash
docker exec -it fraud-kafka kafka-topics \
  --bootstrap-server fraud-kafka:29092 \
  --create \
  --if-not-exists \
  --topic transactions_raw \
  --partitions 1 \
  --replication-factor 1
```

Dans Kafka UI, verifier que le topic `transactions_raw` existe.

## Etape 3 - Entrainer le modele

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  /opt/spark/ml/train_model.py
```

Verifier que les fichiers suivants existent :

- `ml/models/fraud_rf_pipeline`
- `ml/models/metrics.json`

## Etape 4 - Lancer le streaming Spark

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" \
  /opt/spark/fraud_app/jobs/fraud_streaming_job.py
```

Dans Spark UI, verifier que l'application streaming est active.

## Etape 5 - Envoyer des transactions

```bash
python producer/dataset_replay_producer.py
```

Le producer envoie des transactions normales et frauduleuses dans Kafka avec un ratio de fraude configurable via `FRAUD_REPLAY_RATIO`.

Dans Kafka UI, ouvrir le topic `transactions_raw` et verifier les messages JSON.

## Etape 6 - Visualiser dans le dashboard

```bash
streamlit run dashboard/app.py
```

Ouvrir :

```text
http://localhost:8501
```

Points a montrer :

- nombre total de transactions ;
- transactions `NORMAL`, `SUSPICIOUS`, `FRAUD` ;
- montant frauduleux total ;
- histogramme `final_score` ;
- histogramme `ml_probability` ;
- section `Model Evaluation` ;
- dernieres transactions ;
- dernieres alertes.

## Etape 7 - Verifier PostgreSQL

Dans PgAdmin ou via SQL, verifier les tables :

```sql
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM risk_scores;
SELECT COUNT(*) FROM fraud_alerts;
```

Verifier les dernieres transactions :

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

## Etape 8 - Executer les DAGs Airflow

Dans Airflow, lancer dans cet ordre :

```text
repair_missing_alerts_dag
data_quality_dag
daily_fraud_report_dag
model_evaluation_report_dag
```

Verifier les exports dans :

```text
data/reports/
```

Fichiers attendus :

- `repair_missing_alerts_*.csv`
- `data_quality_report_*.csv`
- `fraud_summary_*.csv`
- `fraud_status_distribution_*.csv`
- `fraud_details_*.csv`
- `fraud_top_countries_*.csv`
- `model_evaluation_metrics_*.csv`
- `model_evaluation_distribution_*.csv`
- `model_top_predictions_*.csv`

## Etape 9 - Message de conclusion

La demonstration montre une chaine complete :

```text
Producer -> Kafka -> Spark Streaming -> Hybrid Scoring -> PostgreSQL -> Dashboard + Airflow Reports
```

Points forts a souligner :

- traitement temps reel ;
- scoring hybride explicable ;
- integration ML ;
- stockage structure ;
- dashboard interactif ;
- orchestration Airflow ;
- exports CSV pour audit et reporting.
