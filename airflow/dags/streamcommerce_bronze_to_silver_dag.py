from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from streamcommerce_utils import databricks_or_local_task


@dag(
    dag_id="streamcommerce_bronze_to_silver_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["streamcommerce", "bronze", "silver"],
)
def bronze_to_silver():
    @task
    def bronze_ingestion():
        return databricks_or_local_task("bronze_ingestion", ["python", "-c", "from databricks.src.bronze.ingest import ingest_all; print(ingest_all())"])

    @task
    def silver_transform():
        return databricks_or_local_task("silver_transform", ["python", "-c", "from databricks.src.silver.transform import transform_all; print(transform_all())"])

    @task
    def publish_metrics():
        return "silver metrics published to Airflow logs and Gold DQ scorecard input"

    bronze_ingestion() >> silver_transform() >> publish_metrics()


bronze_to_silver()
