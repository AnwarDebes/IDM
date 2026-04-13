"""
Kafka producer: streams CSV rows as JSON events to raw-disease-events topic.
Simulates real-time feed from health departments.
"""

import argparse
import json
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def produce(limit: int = 0, delay_ms: int = 10):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")

    # Ensure topic exists
    admin = AdminClient({"bootstrap.servers": broker})
    topics = admin.list_topics(timeout=10).topics
    if "raw-disease-events" not in topics:
        admin.create_topics([
            NewTopic("raw-disease-events", num_partitions=3, replication_factor=1),
            NewTopic("raw-demographics", num_partitions=1, replication_factor=1),
            NewTopic("transformed-disease-events", num_partitions=3, replication_factor=1),
        ])
        print("Topics created.")
        time.sleep(2)

    producer = Producer({"bootstrap.servers": broker})

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base, "data", "raw", "tycho_level1.csv")
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)

    if limit > 0:
        df = df.head(limit)

    total = len(df)
    delivered = 0
    errors = 0

    def delivery_cb(err, msg):
        nonlocal delivered, errors
        if err:
            errors += 1
        else:
            delivered += 1

    print(f"Producing {total:,} messages to raw-disease-events...")
    start = time.time()

    for i, (_, row) in enumerate(df.iterrows()):
        event = {
            "epi_week": int(row["epi_week"]),
            "state": str(row["state"]),
            "loc": str(row["loc"]),
            "loc_type": str(row["loc_type"]),
            "disease": str(row["disease"]),
            "cases": int(row["cases"]),
            "incidence_rate": float(row["incidence_per_100000"]),
        }
        key = f"{row['state']}_{row['epi_week']}"
        producer.produce(
            "raw-disease-events",
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_cb,
        )

        if (i + 1) % 10000 == 0:
            producer.flush()
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            print(f"  {i + 1:,} / {total:,} sent ({rate:.0f} msg/s)")

        if delay_ms > 0 and (i + 1) % 100 == 0:
            producer.poll(0)
            time.sleep(delay_ms / 1000.0)

    producer.flush()
    elapsed = time.time() - start
    print(f"\nDone: {delivered:,} delivered, {errors} errors in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limit number of messages (0=all)")
    parser.add_argument("--delay", type=int, default=10, help="Delay between batches in ms")
    args = parser.parse_args()
    produce(limit=args.limit, delay_ms=args.delay)
