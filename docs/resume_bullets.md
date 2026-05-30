# Resume Bullets

- Built an end-to-end real-time e-commerce lakehouse using Kafka, AWS S3, Airflow, Databricks, Delta Lake design patterns, and Plotly Dash.
- Designed Bronze, Silver, and Gold Medallion layers with raw audit retention, schema normalization, deduplication, quarantine handling, and analytics-ready Gold aggregates.
- Implemented a reusable Python data quality framework covering schema, completeness, validity, uniqueness, freshness-ready metrics, and business rule checks.
- Created custom Kafka event simulators for clickstream, orders, payments, inventory, product catalog, and shipment events with configurable event rate, duration, and bad-data percentage.
- Built Gold KPI tables for revenue, average order value, product performance, customer lifetime value, repeat purchase behavior, stockout risk, payment reliability, and DQ pass rate.
- Developed Airflow DAGs for batch ingestion, Bronze-to-Silver processing, Silver-to-Gold aggregation, and daily critical data quality enforcement.
- Containerized the local development stack with Docker Compose, including Kafka, Airflow, PostgreSQL, MinIO, and Dash.
- Added pytest coverage for event generation, data quality checks, Silver transformations, and Gold aggregation logic.
