#!/usr/bin/env bash
# Runs INSIDE the kafka container: make init does
#   docker compose exec -T kafka bash /opt/streamcommerce/scripts/init_kafka_topics.sh
# The official apache/kafka image keeps its CLI tools in /opt/kafka/bin.
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=(customer_events orders payments inventory_updates product_catalog_updates shipment_events)

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

echo "Kafka topics ready: ${TOPICS[*]}"
