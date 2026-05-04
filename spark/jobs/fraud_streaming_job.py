from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    lit,
    hour,
    to_timestamp,
    current_timestamp,
    concat,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


ML_MODEL_PATH = "/opt/spark/ml/models/fraud_rf_pipeline"

KAFKA_BOOTSTRAP_SERVERS = "fraud-kafka:29092"
KAFKA_TOPIC = "transactions_raw"

POSTGRES_URL = "jdbc:postgresql://fraud-postgres:5432/fraud_detection"
POSTGRES_USER = "fraud_user"
POSTGRES_PASSWORD = "fraud_password"

CHECKPOINT_LOCATION = "/opt/spark/fraud_app/checkpoints/fraud_streaming"

ML_FEATURE_COLS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Real-Time Hybrid Fraud Detection Streaming Job")
        .master("spark://fraud-spark-master:7077")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty.")
        return

    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    transactions_df = batch_df.select(
        "transaction_id",
        "user_id",
        "amount",
        "currency",
        "country",
        "usual_country",
        "merchant_category",
        "payment_method",
        "device_id",
        "ip_address",
        col("event_time").alias("transaction_timestamp"),
        "transaction_status",
        "risk_score",
        "ml_probability",
        "behavior_score",
        "final_score",
        "detection_method",
        col("actual_label").cast("integer").alias("actual_label"),
    )

    risk_scores_df = batch_df.select(
        "transaction_id",
        "amount_score",
        "country_score",
        "merchant_score",
        "time_score",
        "device_score",
        "frequency_score",
        col("final_score").alias("final_score"),
        col("transaction_status").alias("decision"),
    )

    fraud_alerts_df = (
        batch_df
        .filter(col("transaction_status").isin("SUSPICIOUS", "FRAUD"))
        .select(
            "transaction_id",
            "user_id",
            "risk_score",
            lit("HYBRID_FRAUD_DETECTION").alias("alert_type"),
            concat(
                lit("Transaction "),
                col("transaction_id"),
                lit(" detected as "),
                col("transaction_status"),
                lit(" | final_score="),
                col("final_score").cast("string"),
                lit(" | ml_probability="),
                col("ml_probability").cast("string"),
            ).alias("message"),
            lit("OPEN").alias("status"),
        )
    )

    transactions_df.write.jdbc(
        url=POSTGRES_URL,
        table="transactions",
        mode="append",
        properties=jdbc_properties,
    )

    risk_scores_df.write.jdbc(
        url=POSTGRES_URL,
        table="risk_scores",
        mode="append",
        properties=jdbc_properties,
    )

    fraud_alerts_df.write.jdbc(
        url=POSTGRES_URL,
        table="fraud_alerts",
        mode="append",
        properties=jdbc_properties,
    )

    print(f"Batch {batch_id} written successfully to PostgreSQL.")


def main():
    spark = create_spark_session()

    print("Loading ML model from:", ML_MODEL_PATH)
    ml_model = PipelineModel.load(ML_MODEL_PATH)
    print("ML model loaded successfully.")

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("usual_country", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),

        StructField("user_avg_amount", DoubleType(), True),
        StructField("amount_vs_avg_user", DoubleType(), True),
        StructField("actual_label", DoubleType(), True),

        StructField("Time", DoubleType(), True),
    ])

    for i in range(1, 29):
        transaction_schema.add(StructField(f"V{i}", DoubleType(), True))

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), transaction_schema).alias("data"))
        .select("data.*")
    )

    cleaned_df = (
        parsed_df
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Amount", col("amount"))
        .withColumn(
            "user_avg_amount",
            when(col("user_avg_amount").isNull(), lit(200.0)).otherwise(col("user_avg_amount"))
        )
        .withColumn(
            "amount_vs_avg_user",
            when(
                col("amount_vs_avg_user").isNull(),
                col("amount") / lit(200.0)
            ).otherwise(col("amount_vs_avg_user"))
        )
        .filter(col("transaction_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("event_time").isNotNull())
        .fillna(0.0, subset=ML_FEATURE_COLS)
    )

    rule_scored_df = (
        cleaned_df
        .withColumn(
            "amount_score",
            when(col("amount") > 10000, 40)
            .when(col("amount") > 5000, 30)
            .otherwise(0)
        )
        .withColumn(
            "country_score",
            when(col("country") != col("usual_country"), 20).otherwise(0)
        )
        .withColumn(
            "merchant_score",
            when(
                col("merchant_category").isin("Crypto", "Luxury", "Gaming", "Gambling"),
                15
            ).otherwise(0)
        )
        .withColumn(
            "time_score",
            when(
                (hour(col("event_time")) >= 0) & (hour(col("event_time")) <= 5),
                10
            ).otherwise(0)
        )
        .withColumn(
            "device_score",
            when(col("device_id").rlike("D-8|D-9"), 15).otherwise(0)
        )
        .withColumn(
            "frequency_score",
            lit(0)
        )
        .withColumn(
            "rule_score",
            col("amount_score")
            + col("country_score")
            + col("merchant_score")
            + col("time_score")
            + col("device_score")
            + col("frequency_score")
        )
        .withColumn(
            "behavior_score",
            when(col("amount_vs_avg_user") >= 10, 30)
            .when(col("amount_vs_avg_user") >= 5, 20)
            .when(col("amount_vs_avg_user") >= 3, 10)
            .otherwise(0)
        )
    )

    ml_df = ml_model.transform(rule_scored_df)

    scored_df = (
        ml_df
        .withColumn(
            "ml_probability",
            vector_to_array(col("probability")).getItem(1)
        )
        .withColumn(
            "ml_score",
            col("ml_probability") * 100
        )
        .withColumn(
            "final_score",
            spark_round(
                (col("rule_score") * 0.35)
                + (col("ml_score") * 0.45)
                + (col("behavior_score") * 0.20)
            ).cast("integer")
        )
        .withColumn(
            "transaction_status",
            when(
                (col("final_score") >= 60) |
                ((col("rule_score") >= 50) & (col("ml_probability") >= 0.10)),
                "FRAUD"
            )
            .when(
                (col("final_score") >= 35) |
                (col("ml_probability") >= 0.08) |
                (col("rule_score") >= 40),
                "SUSPICIOUS"
            )
            .otherwise("NORMAL")
        )

        .withColumn("risk_score", col("final_score"))
        .withColumn("detection_method", lit("HYBRID_RULES_ML_BEHAVIOR"))
        .withColumn("processed_at", current_timestamp())
    )

    query = (
        scored_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print("Spark Streaming hybrid fraud detection job started successfully.")
    print("Reading from Kafka topic:", KAFKA_TOPIC)
    print("Writing results to PostgreSQL.")

    query.awaitTermination()


if __name__ == "__main__":
    main()