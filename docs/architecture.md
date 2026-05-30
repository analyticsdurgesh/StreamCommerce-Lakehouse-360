# Architecture

StreamCommerce Lakehouse 360 is a hybrid local/cloud lakehouse. Local mode uses Docker Compose with Kafka, Airflow, PostgreSQL, MinIO, and Dash. Cloud mode replaces MinIO with AWS S3 and runs Spark/Delta transformations in Databricks.

The design separates concerns:

- Kafka captures operational source events.
- S3/MinIO stores immutable landing data.
- Bronze preserves raw JSON and ingestion metadata.
- Silver parses, deduplicates, validates, standardizes, and quarantines invalid records.
- Gold serves dashboard-ready aggregate tables.
- Airflow orchestrates the lifecycle.
- Data quality checks publish scorecards and quarantine evidence.

Databricks optimization notes: partition by `processing_date` or `event_date`, compact small files after heavy ingestion, Z-order high-cardinality lookup columns such as `customer_id` and `product_id`, vacuum old Delta files according to retention policy, and use streaming checkpoints for production Kafka ingestion.
