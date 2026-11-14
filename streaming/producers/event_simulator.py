from __future__ import annotations

import argparse
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from streaming.topics import TOPICS

try:
    from faker import Faker

    fake = Faker()
except ImportError:
    class _FallbackFaker:
        def email(self) -> str:
            return f"user{random.randint(1000, 9999)}@example.com"

    fake = _FallbackFaker()
LOGGER = logging.getLogger("streamcommerce.event_simulator")

CATEGORIES = ["electronics", "home", "fashion", "beauty", "sports", "books", "grocery"]
DEVICES = ["web", "ios", "android", "tablet"]
LOCATIONS = ["New York", "Austin", "Seattle", "Chicago", "San Francisco", "Atlanta", "Denver"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
INVENTORY_STATUSES = ["IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK", "RESERVED"]


@dataclass(frozen=True)
class EventProfile:
    source: str
    topic: str
    event_types: list[str]


def _timestamp(late_probability: float = 0.05) -> str:
    now = datetime.now(timezone.utc)
    if random.random() < late_probability:
        now -= timedelta(hours=random.randint(2, 72), minutes=random.randint(0, 59))
    return now.replace(microsecond=0).isoformat()


def base_event(profile: EventProfile) -> dict[str, Any]:
    product_id = f"SKU-{random.randint(1000, 1099)}"
    quantity = random.randint(1, 5)
    price = round(random.uniform(8, 450), 2)
    order_id = f"ORD-{random.randint(100000, 999999)}"
    event_type = random.choice(profile.event_types)
    return {
        "event_id": str(uuid4()),
        "event_timestamp": _timestamp(),
        "source_system": profile.source,
        "event_type": event_type,
        "customer_id": f"CUST-{random.randint(10000, 99999)}",
        "session_id": str(uuid4()),
        "product_id": product_id,
        "category": random.choice(CATEGORIES),
        "price": price,
        "quantity": quantity,
        "order_id": order_id,
        "payment_id": f"PAY-{random.randint(100000, 999999)}",
        "payment_status": random.choice(PAYMENT_STATUSES),
        "inventory_status": random.choice(INVENTORY_STATUSES),
        "device_type": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "campaign_id": f"CMP-{random.randint(1, 20):03d}",
        "email": fake.email(),
        "amount": round(price * quantity, 2),
        "warehouse_id": f"WH-{random.randint(1, 8):02d}",
        "seller_id": f"SELLER-{random.randint(1, 50):03d}",
    }


def inject_bad_data(event: dict[str, Any], bad_data_percentage: float) -> dict[str, Any] | str:
    if random.random() * 100 > bad_data_percentage:
        return event
    defect = random.choice(
        [
            "null_customer",
            "negative_price",
            "invalid_payment_status",
            "missing_product",
            "missing_timestamp",
            "schema_drift",
            "malformed_json",
            "negative_quantity",
            "invalid_product",
        ]
    )
    mutated = dict(event)
    if defect == "null_customer":
        mutated["customer_id"] = None
    elif defect == "negative_price":
        mutated["price"] = -abs(float(mutated["price"]))
    elif defect == "invalid_payment_status":
        mutated["payment_status"] = "CHARGEBACK_UNKNOWN"
    elif defect == "missing_product":
        mutated.pop("product_id", None)
    elif defect == "missing_timestamp":
        mutated["event_timestamp"] = None
    elif defect == "schema_drift":
        mutated["unexpected_checkout_score"] = random.random()
    elif defect == "malformed_json":
        return '{"event_id": "broken", "event_timestamp": '
    elif defect == "negative_quantity":
        mutated["quantity"] = -random.randint(1, 5)
    elif defect == "invalid_product":
        mutated["product_id"] = "SKU-DOES-NOT-EXIST"
    mutated["_defect_type"] = defect
    return mutated


def event_profile(source: str) -> EventProfile:
    if source not in TOPICS:
        raise ValueError(f"Unknown source {source}. Choose one of {sorted(TOPICS)}")
    spec = TOPICS[source]
    return EventProfile(source=source, topic=spec["topic"], event_types=spec["events"])


def generate_event(source: str, bad_data_percentage: float = 5.0) -> dict[str, Any] | str:
    profile = event_profile(source)
    return inject_bad_data(base_event(profile), bad_data_percentage)


def run_producer(source: str, bootstrap_servers: str, topic: str | None, event_rate: float, duration: int, bad_data_percentage: float) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Install kafka-python or run inside the Docker environment to produce to Kafka.") from exc

    profile = event_profile(source)
    target_topic = topic or profile.topic
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=3,
    )
    deadline = time.time() + duration if duration > 0 else None
    LOGGER.info("Producing %s events to %s at %.2f events/sec", source, target_topic, event_rate)
    while deadline is None or time.time() < deadline:
        event = generate_event(source, bad_data_percentage)
        payload = event.encode("utf-8") if isinstance(event, str) else event
        producer.send(target_topic, payload)
        producer.flush(timeout=10)
        time.sleep(max(0.0, 1.0 / event_rate))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="StreamCommerce synthetic Kafka event producer")
    parser.add_argument("--source", required=True, choices=sorted(TOPICS))
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--event-rate", type=float, default=5.0)
    parser.add_argument("--duration", type=int, default=60, help="Seconds. Use 0 for continuous mode.")
    parser.add_argument("--bad-data-percentage", type=float, default=8.0)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_producer(
        source=args.source,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        event_rate=args.event_rate,
        duration=args.duration,
        bad_data_percentage=args.bad_data_percentage,
    )
