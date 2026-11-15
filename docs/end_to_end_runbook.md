# StreamCommerce Lakehouse 360 End-to-End Runbook

This guide explains every step needed to run **StreamCommerce Lakehouse 360** locally, understand where the data comes from, generate data, process it through Bronze/Silver/Gold layers, run tests, open the dashboard, and understand the AWS/Databricks production path.

## 1. What This Project Does

StreamCommerce Lakehouse 360 is a production-style portfolio project for a fictional e-commerce company named **StreamCommerce**.

It simulates real operational systems, sends events through Kafka, lands data in S3-compatible storage, builds a Medallion lakehouse, checks data quality, quarantines bad data, and powers an executive dashboard.

The main flow is:

```text
Synthetic source systems
  -> Kafka topics
  -> landing object storage
  -> Bronze raw tables
  -> Silver cleaned tables + quarantine
  -> Gold analytics tables
  -> Plotly Dash dashboard
```

## 2. Where Is The Source Of Data?

The data source is **not a static downloaded CSV by default**. The primary source is a custom-built event simulator inside this repository.

The simulator is located here:

```text
streaming/producers/event_simulator.py
```

It represents six production-like e-commerce source systems:

| Source system | Kafka topic | Event examples |
|---|---|---|
| Website and mobile app clickstream | `customer_events` | `page_view`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `search`, `logout` |
| Order management service | `orders` | `order_created`, `order_confirmed`, `order_cancelled`, `order_returned` |
| Payment gateway service | `payments` | `payment_success`, `payment_failed`, `payment_pending`, `refund_success`, `refund_failed` |
| Inventory management service | `inventory_updates` | `stock_added`, `stock_reserved`, `stock_released`, `stockout_warning`, `inventory_adjusted` |
| Product catalog service | `product_catalog_updates` | `product_created`, `product_updated`, `price_changed`, `category_changed`, `product_deactivated` |
| Shipment and logistics service | `shipment_events` | `shipment_created`, `shipped`, `delayed`, `delivered`, `returned` |

The project also generates batch reference data here:

```text
scripts/seed_reference_data.py
```

That script creates:

```text
data/landing/reference/customers.csv
data/landing/reference/products.csv
data/landing/reference/warehouses.csv
data/landing/reference/sellers.csv
data/landing/reference/locations.csv
data/landing/reference/marketing_campaigns.csv
```

The simulator intentionally creates imperfect data so the data quality layer has meaningful work to do:

- duplicate events
- late-arriving events
- null customer IDs
- invalid product IDs
- negative prices
- invalid payment statuses
- missing timestamps
- schema drift
- malformed JSON records
- out-of-order events

Optional external data source:

- The UCI Online Retail dataset can be added later as historical seed data.
- It is not required.
- The project runs fully using generated local data.

## 3. Prerequisites

Install these before running the full project:

- Python 3.10 or newer
- Docker Desktop
- Docker Compose
- Make
- Git

Optional for cloud mode:

- AWS CLI
- Terraform
- Databricks workspace
- Databricks personal access token

Check versions:

```bash
python --version
docker --version
docker compose version
make --version
```

## 4. Project Structure

Important folders:

```text
configs/              local and cloud configuration
data_contracts/       JSON schemas for source events
streaming/producers/      synthetic Kafka producers
streaming/consumers/      Kafka-to-landing consumer
airflow/dags/         orchestration DAGs
databricks/notebooks/ Databricks-ready notebooks
databricks/src/       reusable Bronze, Silver, Gold, DQ code
dashboard/            Plotly Dash application
tests/                pytest test suite
scripts/              setup and local pipeline scripts
infra/terraform/      AWS S3 infrastructure
docs/                 project documentation
```

## 5. First-Time Local Setup

From the project root:

```bash
cd "/Users/admin/StreamCommerce Lakehouse 360"
```

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create a local environment file:

```bash
cp .env.example .env
```

## 6. Start The Local Infrastructure

Start Kafka, Airflow, PostgreSQL, MinIO, and the dashboard service:

```bash
make up
```

This uses:

```text
docker-compose.yml
```

Useful local URLs:

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `streamcommerce` / `streamcommerce123` |
| Plotly Dash | http://localhost:8050 | no login |
| Kafka | `localhost:9092` | no login |

## 7. Initialize Kafka Topics And Reference Data

Run:

```bash
make init
```

This does two things:

1. Generates reference CSV files under `data/landing/reference`.
2. Creates Kafka topics:

```text
customer_events
orders
payments
inventory_updates
product_catalog_updates
shipment_events
```

You can also seed reference data directly:

```bash
python scripts/seed_reference_data.py
```

## 8. Generate Streaming Events

Open one terminal per producer, or run one source at a time.

Customer clickstream:

```bash
python -m streaming.producers.event_simulator \
  --source customer_events \
  --event-rate 5 \
  --duration 120 \
  --bad-data-percentage 8
```

Orders:

```bash
python -m streaming.producers.event_simulator \
  --source orders \
  --event-rate 3 \
  --duration 120 \
  --bad-data-percentage 10
```

Payments:

```bash
python -m streaming.producers.event_simulator \
  --source payments \
  --event-rate 3 \
  --duration 120 \
  --bad-data-percentage 10
```

Inventory:

```bash
python -m streaming.producers.event_simulator \
  --source inventory_updates \
  --event-rate 2 \
  --duration 120 \
  --bad-data-percentage 8
```

Product catalog:

```bash
python -m streaming.producers.event_simulator \
  --source product_catalog_updates \
  --event-rate 1 \
  --duration 120 \
  --bad-data-percentage 5
```

Shipments:

```bash
python -m streaming.producers.event_simulator \
  --source shipment_events \
  --event-rate 2 \
  --duration 120 \
  --bad-data-percentage 6
```

Important producer arguments:

| Argument | Meaning |
|---|---|
| `--source` | Which simulated source system to run |
| `--event-rate` | Events per second |
| `--duration` | Number of seconds to run; use `0` for continuous |
| `--bad-data-percentage` | Percent of events intentionally corrupted |

## 9. Land Kafka Events Into Local Storage

In another terminal, run the landing consumer:

```bash
python -m streaming.consumers.s3_landing_consumer --landing-root data/landing
```

This writes Kafka messages as JSONL files under:

```text
data/landing/customer_events/
data/landing/orders/
data/landing/payments/
data/landing/inventory_updates/
data/landing/product_catalog_updates/
data/landing/shipment_events/
```

Example partition:

```text
data/landing/orders/processing_date=2026-05-31/events-1030.jsonl
```

## 10. Run The Local Medallion Pipeline

Run:

```bash
make run-pipeline
```

This executes:

```bash
bash scripts/run_local_pipeline.sh
```

The local pipeline runs:

1. Reference data seeding
2. Bronze ingestion
3. Silver cleaning and validation
4. Quarantine writes
5. Gold aggregation

Equivalent direct command:

```bash
python -m databricks.src.local_pipeline
```

## 11. Understand The Output Layers

Local output is written under:

```text
data/
```

### Landing

Raw JSONL files from Kafka:

```text
data/landing/{topic}/processing_date=YYYY-MM-DD/*.jsonl
```

### Bronze

Raw records with ingestion metadata:

```text
data/bronze/{topic}/processing_date=YYYY-MM-DD/
```

Bronze adds:

```text
raw_json
ingestion_timestamp
source_topic
source_file
batch_id
processing_date
```

### Silver

Cleaned, parsed, deduplicated records:

```text
data/silver/{topic}/
```

Silver handles:

- schema parsing
- timestamp normalization
- event deduplication
- invalid price and quantity checks
- payment status normalization
- data quality flags
- quarantine routing

### Quarantine

Bad records with reasons:

```text
data/quarantine/dq_failures/{topic}/
```

Quarantined records include:

```text
check_name
reason_code
```

### Gold

Business-ready analytics tables:

```text
data/gold/daily_revenue/
data/gold/product_performance/
data/gold/customer_360/
data/gold/inventory_health/
data/gold/payment_reliability/
data/gold/data_quality_scorecard/
```

## 12. Open The Dashboard

Run:

```bash
make dashboard
```

Open:

```text
http://localhost:8050
```

Dashboard pages:

- Executive Overview
- Revenue & Orders
- Customer Analytics
- Product Performance
- Inventory Monitoring
- Data Quality Monitor

The dashboard reads Gold output from:

```text
data/gold
```

You can override this path:

```bash
export GOLD_DATA_PATH=data/gold
python dashboard/app.py
```

## 13. Run Airflow DAGs Locally

Start the stack:

```bash
make up
```

Open Airflow:

```text
http://localhost:8080
```

Login:

```text
username: admin
password: admin
```

Enable and trigger DAGs in this order:

1. `streamcommerce_batch_ingestion_dag`
2. `streamcommerce_bronze_to_silver_dag`
3. `streamcommerce_silver_to_gold_dag`
4. `streamcommerce_data_quality_dag`

What each DAG does:

| DAG | Purpose |
|---|---|
| `streamcommerce_batch_ingestion_dag` | Seeds reference data and validates landing availability |
| `streamcommerce_bronze_to_silver_dag` | Runs Bronze ingestion, Silver transforms, DQ checks, and quarantine writes |
| `streamcommerce_silver_to_gold_dag` | Builds Gold aggregates and refreshes dashboard tables |
| `streamcommerce_data_quality_dag` | Runs DQ scorecard checks and fails if critical checks drop below threshold |

## 14. Run Tests

Run all tests:

```bash
make test
```

Or:

```bash
pytest -q
```

Test categories:

```text
tests/unit/          event generation and transformation functions
tests/integration/   Bronze-to-Silver and Gold aggregation flows
tests/data_quality/  reusable data quality framework checks
tests/fixtures/      sample JSONL input data
```

Validation script:

```bash
bash scripts/validate_project.sh
```

## 15. Stop The Local Stack

Run:

```bash
make down
```

This stops Docker containers.

To remove generated local data:

```bash
rm -rf data
```

## 16. Run Without Kafka For A Quick Demo

If you only want to demonstrate the Medallion pipeline quickly, use the included test fixtures.

Create landing folders:

```bash
mkdir -p data/landing/orders/processing_date=2026-05-31
mkdir -p data/landing/payments/processing_date=2026-05-31
```

Copy fixtures:

```bash
cp tests/fixtures/orders.jsonl data/landing/orders/processing_date=2026-05-31/orders.jsonl
cp tests/fixtures/payments.jsonl data/landing/payments/processing_date=2026-05-31/payments.jsonl
```

Run:

```bash
python -m databricks.src.local_pipeline
```

Open dashboard:

```bash
python dashboard/app.py
```

## 17. AWS S3 Production-Style Setup

Local mode uses MinIO. Production mode uses AWS S3.

Provision S3:

```bash
cd infra/terraform
terraform init
terraform apply \
  -var="bucket_name=streamcommerce-lakehouse" \
  -var="aws_region=us-east-1"
```

Expected S3 layout:

```text
s3://streamcommerce-lakehouse/landing/
s3://streamcommerce-lakehouse/bronze/
s3://streamcommerce-lakehouse/silver/
s3://streamcommerce-lakehouse/gold/
s3://streamcommerce-lakehouse/quarantine/
```

Set cloud environment variables:

```bash
export ENVIRONMENT=prod
export AWS_REGION=us-east-1
export S3_BUCKET=streamcommerce-lakehouse
```

Cloud config file:

```text
configs/aws.yaml
```

## 18. Databricks Usage

Databricks notebooks are located here:

```text
databricks/notebooks/01_bronze_ingestion.py
databricks/notebooks/02_silver_cleaning.py
databricks/notebooks/03_gold_aggregations.py
databricks/notebooks/04_data_quality_checks.py
```

Databricks job definition:

```text
databricks/jobs/streamcommerce_lakehouse_job.json
```

Set required environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace-url"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
```

In Databricks:

1. Import or sync this repository into Databricks Repos.
2. Attach notebooks to a cluster with Delta Lake support.
3. Configure widgets or job parameters:

```text
landing_path=s3://streamcommerce-lakehouse/landing
bronze_path=s3://streamcommerce-lakehouse/bronze
silver_path=s3://streamcommerce-lakehouse/silver
gold_path=s3://streamcommerce-lakehouse/gold
quarantine_path=s3://streamcommerce-lakehouse/quarantine
processing_date=YYYY-MM-DD
```

4. Run notebooks in order:

```text
01_bronze_ingestion.py
02_silver_cleaning.py
03_gold_aggregations.py
04_data_quality_checks.py
```

## 19. Data Quality Checks Included

The reusable framework is here:

```text
databricks/src/quality/checks.py
```

It checks:

- required columns exist
- `event_id` is not null
- `event_timestamp` is not null
- `event_id` uniqueness
- `price >= 0`
- `quantity > 0`
- valid `payment_status`
- valid `event_type`
- successful payments have positive amount

Each check produces:

```text
passed
failed_record_count
total_record_count
pass_percentage
severity
reason_code
checked_at
```

The Gold DQ output is:

```text
data/gold/data_quality_scorecard/
```

## 20. What To Show In A Demo

Recommended demo sequence:

1. Show `streaming/producers/event_simulator.py` and explain the source systems.
2. Run a producer with a non-zero bad data percentage.
3. Show landing JSONL files under `data/landing`.
4. Run `python -m databricks.src.local_pipeline`.
5. Show Bronze, Silver, Gold, and quarantine folders.
6. Open the dashboard at `http://localhost:8050`.
7. Open the Data Quality Monitor page.
8. Run `pytest -q`.
9. Explain how the same design maps to S3 and Databricks.

## 21. Common Issues

### Docker is not running

Start Docker Desktop, then run:

```bash
make up
```

### Kafka connection fails

Confirm Kafka is running:

```bash
docker compose ps
```

Restart the stack:

```bash
make down
make up
```

### Dashboard shows sample data

This means Gold tables do not exist yet. Run:

```bash
make run-pipeline
```

### No Gold output appears

Make sure landing data exists:

```bash
find data/landing -type f
```

Then run:

```bash
python -m databricks.src.local_pipeline
```

### Python cannot import local modules

Run commands from the project root:

```bash
cd "/Users/admin/StreamCommerce Lakehouse 360"
```

## 22. Resume Explanation

Use this short explanation:

Built a real-time e-commerce analytics lakehouse using Kafka, S3-compatible object storage, Airflow, Databricks-style Medallion Architecture, reusable data quality checks, quarantine handling, Gold KPI tables, and a Plotly Dash executive dashboard.

## 23. Interview Explanation

Say:

“The source data is generated by a custom event simulator that represents six production systems: clickstream, orders, payments, inventory, product catalog, and shipments. Those events are sent to Kafka, landed as raw JSON in object storage, preserved in Bronze, cleaned and validated in Silver, aggregated in Gold, and visualized in Dash. I intentionally generate bad data so I can demonstrate data quality checks, quarantine handling, and trusted analytics.”

