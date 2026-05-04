import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


DATA_PATH = "/opt/spark/data/raw/creditcard.csv"
MODEL_PATH = "/opt/spark/ml/models/fraud_rf_pipeline"
METRICS_PATH = "/opt/spark/ml/models/metrics.json"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Fraud Detection Random Forest Training")
        .master("spark://fraud-spark-master:7077")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(DATA_PATH)
    )

    feature_cols = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]

    df = df.select(feature_cols + ["Class"]).dropna()

    total_count = df.count()
    fraud_count = df.filter(col("Class") == 1).count()
    normal_count = df.filter(col("Class") == 0).count()

    normal_weight = total_count / (2.0 * normal_count)
    fraud_weight = total_count / (2.0 * fraud_count)

    df = df.withColumn(
        "class_weight",
        when(col("Class") == 1, fraud_weight).otherwise(normal_weight)
    )

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="Class",
        weightCol="class_weight",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=80,
        maxDepth=8,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, rf])

    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="Class",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="Class",
        predictionCol="prediction",
        metricName="f1"
    )

    auc = auc_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)

    model.write().overwrite().save(MODEL_PATH)

    metrics = {
        "model_type": "Spark ML RandomForestClassifier",
        "total_rows": total_count,
        "normal_rows": normal_count,
        "fraud_rows": fraud_count,
        "auc": auc,
        "f1": f1,
        "features": feature_cols,
        "model_path": MODEL_PATH
    }

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=4)

    print("Model trained successfully.")
    print(json.dumps(metrics, indent=4))

    spark.stop()


if __name__ == "__main__":
    main()