from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from streaming.topics import TOPIC_NAMES

LOGGER = logging.getLogger("streamcommerce.s3_landing_consumer")


def landing_path(root: str, topic: str) -> Path:
    dt = datetime.now(timezone.utc)
    return Path(root) / topic / f"processing_date={dt.date().isoformat()}" / f"events-{dt.strftime('%H%M')}.jsonl"


def consume_to_landing(
    bootstrap_servers: str, landing_root: str, topics: list[str], idle_timeout_seconds: float = 0
) -> int:
    """Write every Kafka message to a JSONL landing file and return how many were written.

    idle_timeout_seconds=0 (the default) keeps consuming forever, like a real
    landing service. A positive value stops once no message has arrived for that
    long, which is how CI (and you, if you want to "drain and exit") runs it.
    """
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError("Install kafka-python or run inside the Docker environment to consume Kafka messages.") from exc

    options = {}
    if idle_timeout_seconds > 0:
        options["consumer_timeout_ms"] = int(idle_timeout_seconds * 1000)
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="streamcommerce-landing-writer",
        value_deserializer=lambda b: b.decode("utf-8"),
        **options,
    )
    written = 0
    for message in consumer:
        path = landing_path(landing_root, message.topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = json.loads(message.value)
        except json.JSONDecodeError:
            payload = {"_malformed_record": message.value}
        payload["_kafka_topic"] = message.topic
        payload["_kafka_partition"] = message.partition
        payload["_kafka_offset"] = message.offset
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload) + "\n")
        written += 1
        LOGGER.info("Wrote event from %s offset %s to %s", message.topic, message.offset, path)
    consumer.close()
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--landing-root", default="data/landing")
    parser.add_argument("--topics", nargs="*", default=TOPIC_NAMES)
    parser.add_argument(
        "--idle-timeout-seconds",
        type=float,
        default=0,
        help="Stop after this many seconds without a new message. 0 (default) = run forever.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    total = consume_to_landing(args.bootstrap_servers, args.landing_root, args.topics, args.idle_timeout_seconds)
    LOGGER.info("Landed %s events", total)
