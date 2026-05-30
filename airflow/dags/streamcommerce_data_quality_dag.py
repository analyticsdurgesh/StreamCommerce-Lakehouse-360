from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from streamcommerce_utils import run_local_command


@dag(
    dag_id="streamcommerce_data_quality_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["streamcommerce", "data-quality"],
)
def data_quality():
    @task
    def run_full_scan():
        return run_local_command(["python", "-c", "from databricks.src.gold.aggregate import build_data_quality_scorecard; df=build_data_quality_scorecard('data/silver'); print(df.to_json(orient='records'))"])

    @task
    def enforce_threshold(scorecard_json: str):
        import json

        rows = json.loads(scorecard_json) if scorecard_json.strip().startswith("[") else []
        critical = [r for r in rows if r.get("severity") == "critical"]
        if critical and min(float(r.get("pass_percentage", 0)) for r in critical) < 0.95:
            raise AirflowFailException("Critical data quality pass rate fell below 95%")
        return "critical DQ checks passed"

    enforce_threshold(run_full_scan())


data_quality()
