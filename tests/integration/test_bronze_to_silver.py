from pathlib import Path

from databricks.src.bronze.ingest import ingest_topic_to_bronze
from databricks.src.silver.transform import transform_topic


def test_bronze_to_silver_quarantines_bad_records(tmp_path):
    landing = tmp_path / "landing" / "orders" / "processing_date=2026-05-30"
    landing.mkdir(parents=True)
    fixture = Path("tests/fixtures/orders.jsonl").read_text()
    (landing / "orders.jsonl").write_text(fixture)

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    quarantine = tmp_path / "quarantine"
    bronze_df = ingest_topic_to_bronze(str(tmp_path / "landing"), str(bronze), "orders", "2026-05-30")
    valid, scorecard = transform_topic(str(bronze), str(silver), str(quarantine), "orders")

    assert len(bronze_df) == 3
    assert len(valid) == 1
    assert (scorecard["failed_record_count"] > 0).any()
    assert (quarantine / "dq_failures" / "orders").exists()
