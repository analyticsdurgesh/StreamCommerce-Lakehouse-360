"""Run the four Databricks notebooks on a local Spark session, unchanged, and check them.

Databricks gives a notebook two things for free: `spark` and `dbutils`. This
script provides local stand-ins (widgets resolve to folders in a temp dir), feeds
the notebooks simulator events plus the three records a silver layer must handle
(an event with no event_id, a duplicate event_id, a malformed JSON line), runs
01 -> 02 -> 03 -> 04, and checks what each layer wrote.

Needs Java 17 and:  pip install "pyspark==3.5.3" "delta-spark==3.2.1" faker
Run from the repo root:  python scripts/run_notebooks_locally.py
CI runs it on every push (the spark-notebooks job).
"""
from __future__ import annotations

import json
import random
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from streaming.producers.event_simulator import base_event, event_profile, generate_event  # noqa: E402
from streaming.topics import TOPICS  # noqa: E402

NOTEBOOKS = [
    "01_bronze_ingestion.py",
    "02_silver_cleaning.py",
    "03_gold_aggregations.py",
    "04_data_quality_checks.py",
]
# Serverless compute (Databricks Free Edition) has no RDD API and no SparkContext.
SERVERLESS_UNSAFE = (".rdd", "sparkContext", "input_file_name(")


class _Widgets:
    def __init__(self, values: dict[str, str]):
        self.values = values

    def text(self, name: str, default: str, label: str | None = None) -> None:
        self.values.setdefault(name, default)

    def get(self, name: str) -> str:
        return self.values[name]


class LocalDbutils:
    def __init__(self, values: dict[str, str]):
        self.widgets = _Widgets(values)


def write_landing(landing: Path, per_topic: int = 80) -> dict[str, str]:
    """Simulator events per topic, plus one id-less event, one duplicate, one malformed line."""
    random.seed(20261114)
    planted: dict[str, str] = {}
    for source, spec in TOPICS.items():
        folder = landing / spec["topic"] / "processing_date=2026-11-14"
        folder.mkdir(parents=True, exist_ok=True)
        lines = []
        for _ in range(per_topic):
            event = generate_event(source, bad_data_percentage=10)
            lines.append(event if isinstance(event, str) else json.dumps(event))
        no_id = base_event(event_profile(source))
        no_id["event_id"] = None
        no_id["order_id"] = f"NO-ID-{source}"
        original = base_event(event_profile(source))
        newer = dict(original, event_timestamp="2030-01-01T00:00:00+00:00")
        lines += [json.dumps(no_id), json.dumps(original), json.dumps(newer), '{"event_id": "broken", "event_timestamp": ']
        planted[spec["topic"]] = original["event_id"]
        (folder / "events.jsonl").write_text("\n".join(lines) + "\n")
    return planted


def main() -> int:
    for name in NOTEBOOKS:
        source = (ROOT / "databricks" / "notebooks" / name).read_text()
        bad = [token for token in SERVERLESS_UNSAFE if token in source]
        assert not bad, f"{name} uses {bad}, which serverless compute does not support"

    work = Path(tempfile.mkdtemp(prefix="streamcommerce-notebooks-"))
    paths = {name: str(work / name.split("_")[0]) for name in ("landing_path", "bronze_path", "silver_path", "gold_path", "quarantine_path")}
    planted = write_landing(Path(paths["landing_path"]))

    builder = (
        SparkSession.builder.appName("streamcommerce-notebooks")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        widgets = dict(paths, processing_date="")
        for name in NOTEBOOKS:
            source = (ROOT / "databricks" / "notebooks" / name).read_text()
            exec(compile(source, name, "exec"), {"spark": spark, "dbutils": LocalDbutils(widgets)})
            print(f"ran {name}")

        for topic, duplicate_id in planted.items():
            bronze = spark.read.format("delta").load(f"{paths['bronze_path']}/{topic}")
            silver = spark.read.format("delta").load(f"{paths['silver_path']}/{topic}")
            quarantine = spark.read.format("delta").load(f"{paths['quarantine_path']}/dq_failures/{topic}")
            partitions = {row[0] for row in bronze.select("processing_date").distinct().collect()}
            assert "" not in partitions and None not in partitions, f"{topic}: empty processing_date partition {partitions}"
            assert silver.count() > 0, f"{topic}: silver is empty"
            assert silver.filter("event_id is null").count() == 0, f"{topic}: an event without event_id reached silver"
            assert silver.count() == silver.select("event_id").distinct().count(), f"{topic}: duplicate event_id in silver"
            kept = silver.filter(silver.event_id == duplicate_id)
            assert kept.count() == 1, f"{topic}: the duplicated event is in silver {kept.count()} times"
            assert kept.filter("year(event_timestamp) = 2030").count() == 1, f"{topic}: the newer duplicate was not the one kept"
            assert quarantine.filter("event_id is null").count() >= 2, f"{topic}: the id-less and malformed records are not in quarantine"
            print(f"{topic}: bronze {bronze.count()}, silver {silver.count()}, quarantine {quarantine.count()}")

        gold = paths["gold_path"]
        for table in ("daily_revenue", "product_performance"):
            rows = spark.read.format("delta").load(f"{gold}/{table}").count()
            print(f"gold {table}: {rows} rows")
        scorecard = spark.read.format("delta").load(f"{gold}/data_quality_scorecard")
        assert scorecard.count() == len(TOPICS) * 3, f"scorecard has {scorecard.count()} rows"
        failing = scorecard.filter("NOT passed").select("table_name", "check_name", "failed_record_count").collect()
        assert not failing, f"silver failed its own quality checks: {failing}"
        print(f"data quality scorecard: {scorecard.count()} checks, all passed")
    finally:
        spark.stop()
    print("NOTEBOOKS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
