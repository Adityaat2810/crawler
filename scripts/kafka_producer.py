#!/usr/bin/env python3
"""Simple Kafka producer to feed test URLs into the consumer.

Usage examples:
  python scripts/kafka_producer.py --count 5
  python scripts/kafka_producer.py --bootstrap localhost:9092 --topic urls --count 10
"""

import argparse
import random
import string
from datetime import UTC, datetime

from kafka import KafkaProducer


def random_url() -> str:
    token = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    # Use plain HTTP for the demo target so the in-cluster consumer does not
    # depend on the local environment's custom TLS trust chain.
    return f"http://example.com/?id={token}&ts={int(datetime.now(UTC).timestamp())}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Send test URL messages to Kafka")
    parser.add_argument("--bootstrap", default="localhost:9093", help="Kafka bootstrap server (host:port). Use localhost:9093 when running against minikube via skaffold port-forward.")
    parser.add_argument("--topic", default="urls", help="Kafka topic name")
    parser.add_argument("--count", type=int, default=5, help="How many messages to send")
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: v.encode("utf-8"))

    for i in range(args.count):
        url = random_url()
        future = producer.send(args.topic, url)
        metadata = future.get(timeout=10)
        print(f"[{i+1}/{args.count}] sent {url} -> partition={metadata.partition} offset={metadata.offset}")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
