from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from streamcommerce_utils import databricks_or_local_task


@dag(
    dag_id="streamcommerce_silver_to_gold_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["streamcommerce", "gold"],
)
def silver_to_gold():
    @task
    def gold_aggregations():
        return databricks_or_local_task("gold_aggregations", ["python", "-c", "from databricks.src.gold.aggregate import build_all; print(build_all())"])

    @task
    def refresh_dashboard_extracts():
        return "dashboard reads the refreshed Gold parquet files from data/gold or S3 extracts"

    gold_aggregations() >> refresh_dashboard_extracts()


silver_to_gold()
