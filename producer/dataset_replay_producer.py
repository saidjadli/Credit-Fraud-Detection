import json
import os
import random
import time
import uuid
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions_raw")
DATASET_PATH = os.getenv("DATASET_PATH", "data/raw/creditcard.csv")
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.5"))

# Pour la démo : 20% de vraies fraudes du dataset, 80% normales
FRAUD_REPLAY_RATIO = float(os.getenv("FRAUD_REPLAY_RATIO", "0.20"))


COUNTRIES = [
    "Morocco",
    "France",
    "Spain",
    "Germany",
    "USA",
    "Russia",
    "Brazil",
    "China",
]

NORMAL_MERCHANT_CATEGORIES = [
    "Grocery",
    "Restaurant",
    "Online Shopping",
    "Electronics",
]

RISKY_MERCHANT_CATEGORIES = [
    "Crypto",
    "Luxury",
    "Gaming",
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "Mobile Wallet",
    "Bank Transfer",
]

USERS = [
    {"user_id": "U001", "usual_country": "Morocco", "avg_amount": 150},
    {"user_id": "U002", "usual_country": "France", "avg_amount": 220},
    {"user_id": "U003", "usual_country": "Spain", "avg_amount": 180},
    {"user_id": "U004", "usual_country": "Germany", "avg_amount": 300},
    {"user_id": "U005", "usual_country": "Morocco", "avg_amount": 120},
]


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )


def build_transaction(row):
    user = random.choice(USERS)

    actual_label = int(row["Class"])
    amount = float(row["Amount"])

    if actual_label == 1:
        country = random.choice(["Russia", "Brazil", "China", "USA"])
        merchant_category = random.choice(RISKY_MERCHANT_CATEGORIES)
        device_id = "D-" + str(random.randint(800, 999))
    else:
        country = user["usual_country"]
        merchant_category = random.choice(NORMAL_MERCHANT_CATEGORIES)
        device_id = "D-" + str(random.randint(100, 799))

    amount_vs_avg_user = amount / max(float(user["avg_amount"]), 1.0)

    transaction = {
        "transaction_id": "TX-" + str(uuid.uuid4()),
        "user_id": user["user_id"],
        "amount": amount,
        "currency": "EUR",
        "country": country,
        "usual_country": user["usual_country"],
        "merchant_category": merchant_category,
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": device_id,
        "ip_address": f"192.168.1.{random.randint(1, 255)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_avg_amount": float(user["avg_amount"]),
        "amount_vs_avg_user": float(amount_vs_avg_user),
        "actual_label": actual_label,
        "Time": float(row["Time"]),
    }

    for i in range(1, 29):
        transaction[f"V{i}"] = float(row[f"V{i}"])

    return transaction


def choose_row(normal_df, fraud_df):
    should_send_fraud = random.random() < FRAUD_REPLAY_RATIO

    if should_send_fraud and not fraud_df.empty:
        return fraud_df.sample(1).iloc[0]

    return normal_df.sample(1).iloc[0]


def main():
    producer = create_kafka_producer()

    df = pd.read_csv(DATASET_PATH)

    if "Class" not in df.columns:
        raise ValueError("Column 'Class' not found in dataset.")

    normal_df = df[df["Class"] == 0].reset_index(drop=True)
    fraud_df = df[df["Class"] == 1].reset_index(drop=True)

    if normal_df.empty:
        raise ValueError("No normal transactions found in dataset.")

    if fraud_df.empty:
        raise ValueError("No fraud transactions found in dataset.")

    print("Dataset replay producer started.")
    print(f"Dataset path: {DATASET_PATH}")
    print(f"Total rows loaded: {len(df)}")
    print(f"Normal rows: {len(normal_df)}")
    print(f"Fraud rows: {len(fraud_df)}")
    print(f"Fraud replay ratio: {FRAUD_REPLAY_RATIO}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC_TRANSACTIONS}")

    sent_count = 0
    sent_normal_count = 0
    sent_fraud_count = 0

    while True:
        row = choose_row(normal_df, fraud_df)
        transaction = build_transaction(row)

        producer.send(
            topic=KAFKA_TOPIC_TRANSACTIONS,
            key=transaction["transaction_id"],
            value=transaction,
        )

        producer.flush()

        sent_count += 1

        if transaction["actual_label"] == 1:
            sent_fraud_count += 1
        else:
            sent_normal_count += 1

        print(
            f"Sent #{sent_count} | "
            f"transaction_id={transaction['transaction_id']} | "
            f"amount={transaction['amount']} | "
            f"actual_label={transaction['actual_label']} | "
            f"country={transaction['country']} | "
            f"merchant={transaction['merchant_category']} | "
            f"normal_sent={sent_normal_count} | "
            f"fraud_sent={sent_fraud_count}"
        )

        time.sleep(PRODUCER_DELAY_SECONDS)


if __name__ == "__main__":
    main()