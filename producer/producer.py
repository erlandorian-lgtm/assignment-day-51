import json
import random
from datetime import datetime, timezone, timedelta
from time import sleep

from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers="localhost:9093")

user_list = [f"U{i}" for i in range(1001, 1025)]
source_list = ["mobile", "web", "pos"]
invalid_source_list = ["", "ATM", "Unknown"]
invalid_amounts = [-500, -10000, 100000001, 500000000]
invalid_timestamps = ["", "yesterday", "Unknown"]


def generate_transaction():
    if random.random() < 0.2:
        random_amount = random.choice(invalid_amounts)
    else:
        random_amount = random.randint(1, 10_000_000)

    selected_sources = (
        invalid_source_list if random.random() < 0.2 else source_list
    )

    if random.random() < 0.2:
        selected_timestamp = random.choice(invalid_timestamps)
    elif random.random() < 0.15:
        selected_timestamp = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
    else:
        selected_timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "user_id": random.choice(user_list),
        "amount": random_amount,
        "timestamp": selected_timestamp,
        "source": random.choice(selected_sources),
    }


def on_success(metadata):
    print(
        f"Delivered to {metadata.topic} "
        f"[partition {metadata.partition}] "
        f"@ offset {metadata.offset}"
    )


def on_error(exception):
    print(f"Delivery failed: {exception}")


def main():
    transactions = []

    try:
        for i in range(25):
            # Send three exact duplicates to exercise deduplication.
            if i in [10, 15, 20]:
                transaction = transactions[i - 1].copy()
            else:
                transaction = generate_transaction()

            transactions.append(transaction)

            future = producer.send(
                "transactions",
                value=json.dumps(transaction).encode("utf-8"),
            )
            future.add_callback(on_success)
            future.add_errback(on_error)

            sleep(2)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
