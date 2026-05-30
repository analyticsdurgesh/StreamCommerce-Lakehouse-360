from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from streamcommerce_utils import run_local_command


@dag(
    dag_id="streamcommerce_batch_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    tags=["streamcommerce", "landing", "batch"],
)
def streamcommerce_batch_ingestion():
    @task
    def seed_reference_data():
        return run_local_command(["python", "scripts/seed_reference_data.py"])

    @task
    def validate_landing():
        return run_local_command(["python", "-c", "from pathlib import Path; assert Path('data/landing/reference/products.csv').exists(); print('landing reference data ready')"])

    seed_reference_data() >> validate_landing()


streamcommerce_batch_ingestion()
