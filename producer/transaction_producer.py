import json
import os
import random
import time
import uuid
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions_raw")
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "2"))


USERS = [
    {"user_id": "U001", "usual_country": "Morocco"},
    {"user_id": "U002", "usual_country": "France"},
    {"user_id": "U003", "usual_country": "Spain"},
    {"user_id": "U004", "usual_country": "Germany"},
    {"user_id": "U005", "usual_country": "Morocco"},
]

COUNTRIES = [
    "Morocco",
    "France",
    "Spain",
    "Germany",
    "USA",
    "Russia",
    "Brazil",
    "China",
    "Nigeria",
]

NORMAL_MERCHANT_CATEGORIES = [
    "Grocery",
    "Restaurant",
    "Online Shopping",
    "Transport",
    "Pharmacy",
]

RISKY_MERCHANT_CATEGORIES = [
    "Crypto",
    "Luxury",
    "Gaming",
    "Gambling",
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "Mobile Wallet",
    "Bank Transfer",
]


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )


def generate_transaction():
    user = random.choice(USERS)

    is_fraud_like = random.random() < 0.20

    if is_fraud_like:
        amount = round(random.uniform(5000, 15000), 2)
        country = random.choice(["USA", "Russia", "Brazil", "China", "Nigeria"])
        merchant_category = random.choice(RISKY_MERCHANT_CATEGORIES)
    else:
        amount = round(random.uniform(10, 1000), 2)
        country = user["usual_country"]
        merchant_category = random.choice(NORMAL_MERCHANT_CATEGORIES)

    transaction = {
        "transaction_id": "TX-" + str(uuid.uuid4()),
        "user_id": user["user_id"],
        "amount": amount,
        "currency": "EUR",
        "country": country,
        "usual_country": user["usual_country"],
        "merchant_category": merchant_category,
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": "D-" + str(random.randint(100, 999)),
        "ip_address": f"192.168.1.{random.randint(1, 255)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    return transaction


def main():
    producer = create_kafka_producer()

    print("Producer started successfully.")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC_TRANSACTIONS}")

    while True:
        transaction = generate_transaction()

        producer.send(
            topic=KAFKA_TOPIC_TRANSACTIONS,
            key=transaction["transaction_id"],
            value=transaction,
        )

        producer.flush()

        print("Transaction sent to Kafka:")
        print(json.dumps(transaction, indent=4))

        time.sleep(PRODUCER_DELAY_SECONDS)


if __name__ == "__main__":
    main()