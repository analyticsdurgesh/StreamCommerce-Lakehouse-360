#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=(customer_events orders payments inventory_updates product_catalog_updates shipment_events)

for topic in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

echo "Kafka topics ready: ${TOPICS[*]}"
