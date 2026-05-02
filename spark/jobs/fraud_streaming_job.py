from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    lit,
    hour,
    to_timestamp,
    current_timestamp,
    concat,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = "fraud-kafka:29092"
KAFKA_TOPIC = "transactions_raw"

POSTGRES_URL = "jdbc:postgresql://fraud-postgres:5432/fraud_detection"
POSTGRES_USER = "fraud_user"
POSTGRES_PASSWORD = "fraud_password"

CHECKPOINT_LOCATION = "/opt/spark/fraud_app/checkpoints/fraud_streaming"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Real-Time Fraud Detection Streaming Job")
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
    )

    risk_scores_df = batch_df.select(
        "transaction_id",
        "amount_score",
        "country_score",
        "merchant_score",
        "time_score",
        "device_score",
        "frequency_score",
        col("risk_score").alias("final_score"),
        col("transaction_status").alias("decision"),
    )

    fraud_alerts_df = (
        batch_df
        .filter(col("transaction_status").isin("SUSPICIOUS", "FRAUD"))
        .select(
            "transaction_id",
            "user_id",
            "risk_score",
            lit("REAL_TIME_FRAUD_DETECTION").alias("alert_type"),
            concat(
                lit("Transaction "),
                col("transaction_id"),
                lit(" detected as "),
                col("transaction_status"),
                lit(" with risk score "),
                col("risk_score").cast("string"),
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
    ])

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
        .filter(col("transaction_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    scored_df = (
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
            "risk_score",
            col("amount_score")
            + col("country_score")
            + col("merchant_score")
            + col("time_score")
            + col("device_score")
            + col("frequency_score")
        )
        .withColumn(
            "transaction_status",
            when(col("risk_score") >= 70, "FRAUD")
            .when(col("risk_score") >= 40, "SUSPICIOUS")
            .otherwise("NORMAL")
        )
        .withColumn("processed_at", current_timestamp())
    )

    query = (
        scored_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print("Spark Streaming job started successfully.")
    print("Reading from Kafka topic:", KAFKA_TOPIC)
    print("Writing results to PostgreSQL.")

    query.awaitTermination()


if __name__ == "__main__":
    main()