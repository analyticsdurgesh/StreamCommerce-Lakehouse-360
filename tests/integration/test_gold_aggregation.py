from pathlib import Path

from databricks.src.bronze.ingest import ingest_topic_to_bronze
from databricks.src.gold.aggregate import build_all
from databricks.src.silver.transform import transform_topic


def _load_fixture(tmp_path, topic):
    landing = tmp_path / "landing" / topic / "processing_date=2026-05-30"
    landing.mkdir(parents=True)
    landing.joinpath(f"{topic}.jsonl").write_text(Path(f"tests/fixtures/{topic}.jsonl").read_text())


def test_gold_aggregation_builds_daily_revenue(tmp_path):
    for topic in ["orders", "payments"]:
        _load_fixture(tmp_path, topic)
        ingest_topic_to_bronze(str(tmp_path / "landing"), str(tmp_path / "bronze"), topic, "2026-05-30")
        transform_topic(str(tmp_path / "bronze"), str(tmp_path / "silver"), str(tmp_path / "quarantine"), topic)

    counts = build_all(str(tmp_path / "silver"), str(tmp_path / "gold"))
    assert counts["daily_revenue"] >= 1
    assert counts["data_quality_scorecard"] >= 1
