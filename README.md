# Real-Time Credit Fraud Detection Platform

Plateforme Big Data de detection de fraude bancaire en temps reel. Le projet combine Kafka, Spark Structured Streaming, PostgreSQL, Airflow, Streamlit et un modele Machine Learning afin de detecter, scorer, stocker, visualiser et auditer des transactions suspectes.

## Presentation du projet

Ce projet simule une chaine complete de detection de fraude sur des transactions de carte bancaire.

L'objectif est de traiter un flux de transactions en temps reel, de calculer un score de risque hybride, puis de classer chaque transaction en trois statuts :

- `NORMAL` : transaction consideree comme saine.
- `SUSPICIOUS` : transaction a surveiller ou a analyser.
- `FRAUD` : transaction fortement suspecte, avec generation d'alerte.

La plateforme couvre le cycle complet :

- ingestion des transactions via Kafka ;
- traitement streaming avec Apache Spark ;
- scoring par regles, comportement utilisateur et modele ML ;
- stockage dans PostgreSQL ;
- visualisation temps reel avec Streamlit ;
- reporting, qualite de donnees et evaluation via Airflow.

## Architecture

Le flux global est le suivant :

```text
Dataset / Simulateur
        |
        v
Producer Python
        |
        v
Kafka topic: transactions_raw
        |
        v
Spark Structured Streaming
        |
        +--> Rule-based scoring
        +--> Behavior scoring
        +--> ML probability
        +--> Hybrid final_score
        |
        v
PostgreSQL
        |
        +--> Streamlit Dashboard
        +--> Airflow DAGs
        +--> CSV reports
```

Schema visuel :

![Architecture](docs\architecture.png)

Services principaux exposes en local :

| Service | URL |
|---|---|
| Spark Master | http://localhost:18081 |
| Spark Worker | http://localhost:18082 |
| Kafka UI | http://localhost:8085 |
| PgAdmin | http://localhost:5050 |
| Airflow | http://localhost:8088 |
| Streamlit Dashboard | http://localhost:8501 |

## Technologies utilisees

| Technologie | Role |
|---|---|
| Docker Compose | Orchestration locale des services |
| Apache Kafka | Bus de messages temps reel |
| Zookeeper | Coordination Kafka |
| Apache Spark 3.5.1 | Traitement streaming et entrainement ML |
| Spark MLlib | Modele Random Forest |
| PostgreSQL 16 | Stockage des transactions, scores et alertes |
| PgAdmin | Administration PostgreSQL |
| Apache Airflow 2.9.3 | Orchestration des rapports et controles |
| Streamlit | Dashboard analytique temps reel |
| Plotly | Visualisations interactives |
| Python | Producers, dashboard, DAGs, scripts ML |
| pandas / psycopg2 | Reporting, requetes SQL et exports CSV |

## Structure du projet

```text
credit-fraud-detection/
|
|-- airflow/
|   `-- dags/
|       |-- daily_fraud_report_dag.py
|       |-- data_quality_dag.py
|       |-- model_evaluation_report_dag.py
|       `-- repair_missing_alerts_dag.py
|
|-- dashboard/
|   |-- app.py
|   `-- requirements.txt
|
|-- data/
|   |-- raw/
|   |   `-- creditcard.csv
|   `-- reports/
|       |-- fraud_summary_*.csv
|       |-- data_quality_report_*.csv
|       `-- model_evaluation_*.csv
|
|-- docs/
|   |-- architecture.png
|   |-- screenshots/
|   |   |-- dashboard_overview.png
|   |   |-- model_evaluation.png
|   |   |-- airflow_dags.png
|   |   `-- kafka_messages.png
|   `-- demo_scenario.md
|
|-- ml/
|   |-- train_model.py
|   `-- models/
|       |-- fraud_rf_pipeline/
|       `-- metrics.json
|
|-- postgres/
|   `-- init.sql
|
|-- producer/
|   |-- dataset_replay_producer.py
|   |-- transaction_producer.py
|   `-- requirements.txt
|
|-- spark/
|   |-- Dockerfile
|   |-- requirements.txt
|   `-- jobs/
|       `-- fraud_streaming_job.py
|
|-- docker-compose.yml
|-- .env
`-- README.md
```

## Installation

### Prerequis

- Docker Desktop ou Docker Engine avec Docker Compose.
- Python 3.10+ pour lancer le producer et le dashboard en local.
- Git.
- Dataset `creditcard.csv` place dans `data/raw/creditcard.csv`.

### Demarrage de l'environnement

Construire et lancer les services :

```bash
docker compose up -d --build
```

Verifier les conteneurs :

```bash
docker compose ps
```

Creer le topic Kafka si necessaire :

```bash
docker exec -it fraud-kafka kafka-topics \
  --bootstrap-server fraud-kafka:29092 \
  --create \
  --if-not-exists \
  --topic transactions_raw \
  --partitions 1 \
  --replication-factor 1
```

Installer les dependances Python locales :

```bash
pip install -r producer/requirements.txt
pip install -r dashboard/requirements.txt
```

## Lancement

### 1. Entrainer le modele ML

Le modele Random Forest est entraine avec Spark sur le dataset `creditcard.csv`.

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  /opt/spark/ml/train_model.py
```

Le modele est sauvegarde dans :

```text
ml/models/fraud_rf_pipeline
```

Les metriques sont sauvegardees dans :

```text
ml/models/metrics.json
```

### 2. Lancer le job Spark Streaming

```bash
docker exec -it fraud-spark-master /opt/spark/bin/spark-submit \
  --master spark://fraud-spark-master:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" \
  /opt/spark/fraud_app/jobs/fraud_streaming_job.py
```

### 3. Lancer le producer Kafka

Mode recommande pour la demo, avec labels reels du dataset :

```bash
python producer/dataset_replay_producer.py
```

Mode simulation simple :

```bash
python producer/transaction_producer.py
```

### 4. Lancer le dashboard

```bash
streamlit run dashboard/app.py
```

Puis ouvrir :

```text
http://localhost:8501
```

### 5. Utiliser Airflow

Airflow est disponible sur :

```text
http://localhost:8088
```

Identifiants par defaut :

```text
username: admin
password: admin
```

## Fonctionnement du pipeline

### 1. Generation ou replay des transactions

Deux producteurs sont disponibles :

- `producer/transaction_producer.py` genere des transactions synthetiques.
- `producer/dataset_replay_producer.py` rejoue le dataset `creditcard.csv` en enrichissant les lignes avec des champs metier : utilisateur, pays, categorie marchand, moyen de paiement, device, IP et label reel.

Le producer envoie les messages JSON dans le topic Kafka :

```text
transactions_raw
```

### 2. Ingestion Kafka par Spark

Le job `spark/jobs/fraud_streaming_job.py` lit le topic Kafka en streaming, parse le JSON, nettoie les champs essentiels et transforme le timestamp en colonne exploitable.

Les transactions invalides ou incompletes sont filtrees :

- `transaction_id` obligatoire ;
- `user_id` obligatoire ;
- `amount` obligatoire ;
- `event_time` obligatoire.

### 3. Scoring et decision

Spark calcule ensuite :

- un score par regles metier ;
- un score comportemental ;
- une probabilite ML ;
- un score final hybride ;
- un statut final : `NORMAL`, `SUSPICIOUS` ou `FRAUD`.

### 4. Stockage PostgreSQL

Les resultats sont ecrits dans trois tables :

- `transactions` : transactions enrichies et decision finale ;
- `risk_scores` : details des composantes de scoring ;
- `fraud_alerts` : alertes pour les transactions suspectes ou frauduleuses.

## Explication du scoring hybride

Le scoring hybride combine trois approches complementaires.

### 1. Rule-based scoring

Des regles metier attribuent des points selon des signaux de risque :

| Signal | Regle | Score |
|---|---:|---:|
| Montant tres eleve | `amount > 10000` | 40 |
| Montant eleve | `amount > 5000` | 30 |
| Pays inhabituel | `country != usual_country` | 20 |
| Categorie marchand risquee | Crypto, Luxury, Gaming, Gambling | 15 |
| Horaire nocturne | entre 00h et 05h | 10 |
| Device suspect | device contenant `D-8` ou `D-9` | 15 |
| Frequence | reserve pour extension future | 0 |

Ces signaux forment le `rule_score`.

### 2. Behavior scoring

Le score comportemental compare le montant de la transaction au montant moyen habituel de l'utilisateur :

| Ratio `amount_vs_avg_user` | Score |
|---:|---:|
| `>= 10` | 30 |
| `>= 5` | 20 |
| `>= 3` | 10 |
| `< 3` | 0 |

Ce score permet de detecter une transaction anormale meme si elle ne suffit pas seule a declencher une alerte.

### 3. ML probability

Le modele Spark ML utilise un `RandomForestClassifier` entraine sur les variables :

```text
Time, V1, V2, ..., V28, Amount
```

Le modele produit `ml_probability`, c'est-a-dire la probabilite que la transaction soit frauduleuse.

### 4. Score final

Le score final combine les trois signaux :

```text
final_score = 0.35 * rule_score
            + 0.45 * ml_score
            + 0.20 * behavior_score
```

Avec :

```text
ml_score = ml_probability * 100
```

### 5. Decision finale

Une transaction est classee `FRAUD` si :

```text
final_score >= 60
```

ou si :

```text
rule_score >= 50 et ml_probability >= 0.10
```

Une transaction est classee `SUSPICIOUS` si :

```text
final_score >= 35
```

ou si :

```text
ml_probability >= 0.08
```

ou si :

```text
rule_score >= 40
```

Sinon, elle est classee `NORMAL`.

## Airflow DAGs

Les DAGs Airflow automatisent le reporting, la qualite et la maintenance.

| DAG | Frequence | Role |
|---|---|---|
| `daily_fraud_report_dag` | quotidienne | Genere les rapports globaux de fraude |
| `data_quality_dag` | quotidienne | Verifie la coherence des donnees |
| `model_evaluation_report_dag` | quotidienne | Calcule precision, recall, F1-score et accuracy |
| `repair_missing_alerts_dag` | manuel | Repare les alertes manquantes |

Ordre recommande pour une demonstration :

```text
repair_missing_alerts_dag
data_quality_dag
daily_fraud_report_dag
model_evaluation_report_dag
```

Les rapports sont exportes dans :

```text
data/reports/
```

Exemples de fichiers generes :

- `fraud_summary_*.csv`
- `fraud_status_distribution_*.csv`
- `fraud_details_*.csv`
- `fraud_top_countries_*.csv`
- `data_quality_report_*.csv`
- `model_evaluation_metrics_*.csv`
- `model_evaluation_distribution_*.csv`
- `model_top_predictions_*.csv`

## Dashboard

Le dashboard Streamlit permet de suivre le pipeline en temps reel.

Fonctionnalites principales :

- KPIs : total transactions, normales, suspectes, fraudes, montant frauduleux ;
- moyennes de `ml_probability`, `behavior_score` et `final_score` ;
- repartition des statuts ;
- top pays suspects ;
- histogrammes de `final_score` et `ml_probability` ;
- distribution du score comportemental ;
- evaluation modele : precision, recall, F1-score, accuracy ;
- matrice `actual_label` vs `transaction_status` ;
- timeline des fraudes par minute ;
- dernieres transactions traitees ;
- dernieres alertes.

Captures prevues :

![Dashboard overview](docs/screenshots/dashboard_overview.png)

![Model evaluation](docs/screenshots/model_evaluation.png)

## Resultats attendus

### Modele ML

Les metriques sauvegardees dans `ml/models/metrics.json` indiquent :

| Metrique | Valeur |
|---|---:|
| Type modele | Spark ML RandomForestClassifier |
| Total lignes | 284807 |
| Transactions normales | 284315 |
| Fraudes | 492 |
| AUC | 0.9687 |
| F1 | 0.9993 |

### Evaluation hybride observee

Le rapport `data/reports/model_evaluation_metrics_2026-05-04_14-08-36.csv` contient :

| Metrique | Valeur |
|---|---:|
| True positives | 168 |
| False positives | 38 |
| True negatives | 521 |
| False negatives | 0 |
| Precision | 0.8155 |
| Recall | 1.0000 |
| F1-score | 0.8984 |
| Accuracy | 0.9477 |

Interpretation :

- Le recall est excellent dans cette execution : aucune fraude labelisee n'a ete ratee.
- La precision montre qu'il existe encore des faux positifs.
- Ce comportement est coherent pour un systeme de fraude : il vaut souvent mieux analyser trop d'alertes que laisser passer une fraude.

## Limites et ameliorations futures

Limites actuelles :

- le dashboard se connecte directement a PostgreSQL ;
- les identifiants sont simples et adaptes a une demo locale ;
- le score de frequence est reserve mais pas encore implemente ;
- le modele n'est pas reentraine automatiquement par Airflow ;
- les labels reels `actual_label` ne sont disponibles que dans le mode replay dataset ;
- pas encore d'API backend dediee entre le dashboard et la base ;
- pas de gestion avancee des roles analyste/admin ;
- pas de monitoring production type Prometheus/Grafana ;
- pas de schema registry Kafka.

Ameliorations possibles :

- ajouter une API backend securisee, par exemple Spring Boot ou FastAPI ;
- ajouter une authentification dashboard ;
- implementer le score de frequence par fenetre temporelle Spark ;
- ajouter un DAG de reentrainement ML ;
- versionner les modeles avec MLflow ;
- ajouter des tests automatises de qualite data et pipeline ;
- ajouter un schema registry pour valider les messages Kafka ;
- exposer les metriques techniques avec Prometheus ;
- ajouter une interface analyste pour fermer, commenter ou escalader les alertes ;
- deployer la plateforme sur Kubernetes.

## Documentation complementaire

- Scenario de demo : [docs/demo_scenario.md](docs/demo_scenario.md)
- Captures d'ecran : `docs/screenshots/`
- Rapports CSV : `data/reports/`
