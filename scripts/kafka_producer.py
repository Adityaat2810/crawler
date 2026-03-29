#!/usr/bin/env python3
"""Simple Kafka producer to feed test URLs into the crawl frontier.

Usage examples:
  python scripts/kafka_producer.py --bootstrap localhost:9093 --topic frontier.ready
"""

import argparse
import json
import uuid
from datetime import UTC, datetime
from typing import Iterable, List

from kafka import KafkaProducer


DEFAULT_SEEDS: List[str] = [
    "https://example.com/",
    "https://www.iana.org/domains/reserved",
    "https://www.wikipedia.org/",
]


def build_job(url: str, attempt: int = 1) -> bytes:
    payload = {
        "url": url,
        "attempt": attempt,
        "job_id": str(uuid.uuid4()),
        "enqueued_at": datetime.now(UTC).isoformat(),
    }
    return json.dumps(payload).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed frontier.ready with test URLs")
    parser.add_argument(
        "--bootstrap",
        default="localhost:9093",
        help="Kafka bootstrap server (host:port). Use localhost:9093 when running against minikube via skaffold port-forward.",
    )
    parser.add_argument("--topic", default="frontier.ready", help="Kafka topic name")
    parser.add_argument(
        "--urls",
        nargs="*",
        default=DEFAULT_SEEDS,
        help="Space-separated list of URLs to enqueue. Defaults to a few public sites.",
    )
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)

    urls: List[str] = list(args.urls or DEFAULT_SEEDS)
    total = len(urls)
    for i, url in enumerate(urls, start=1):
        future = producer.send(args.topic, build_job(url))
        metadata = future.get(timeout=10)
        print(f"[{i}/{total}] sent {url} -> partition={metadata.partition} offset={metadata.offset}")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
