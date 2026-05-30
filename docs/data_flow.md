# Data Flow

1. Synthetic source services emit JSON events to Kafka topics.
2. The landing consumer writes messages to S3 or MinIO JSONL partitions.
3. Bronze ingestion keeps raw JSON and adds metadata such as ingestion timestamp, source topic, source file, batch ID, and processing date.
4. Silver transforms parse fields, standardize timestamps, deduplicate event IDs, normalize payment statuses, validate required fields, and write invalid records to quarantine.
5. Gold aggregations produce revenue, customer, product, inventory, payment reliability, and DQ scorecard tables.
6. Plotly Dash reads the Gold layer and presents executive analytics.
