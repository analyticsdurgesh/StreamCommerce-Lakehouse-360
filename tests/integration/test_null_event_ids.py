"""Regression tests: rows without an event_id must never reach silver.

Before the fix, silver removed failed rows by matching event_ids. A row whose
event_id is null (or a malformed record with no event_id at all) matched no id,
so it slipped into silver, and deduplication merged all id-less rows into one.
"""
import json

from databricks.src.bronze.ingest import ingest_topic_to_bronze
from databricks.src.silver.transform import transform_topic

GOOD = {
    "event_id": "evt-ok",
    "event_timestamp": "2026-05-30T10:00:00+00:00",
    "source_system": "orders",
    "event_type": "order_created",
    "customer_id": "CUST-1",
    "product_id": "SKU-1000",
    "category": "electronics",
    "price": 100.0,
    "quantity": 1,
    "order_id": "ORD-1",
    "payment_status": "SUCCESS",
    "amount": 100.0,
}


def _run(tmp_path, lines):
    landing = tmp_path / "landing" / "orders" / "processing_date=2026-05-30"
    landing.mkdir(parents=True)
    (landing / "orders.jsonl").write_text("\n".join(lines) + "\n")
    bronze, silver, quarantine = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    ingest_topic_to_bronze(str(tmp_path / "landing"), str(bronze), "orders", "2026-05-30")
    return transform_topic(str(bronze), str(silver), str(quarantine), "orders")


def test_null_event_ids_are_quarantined_not_loaded(tmp_path):
    no_id_a = dict(GOOD, event_id=None, order_id="ORD-2")
    no_id_b = dict(GOOD, event_id=None, order_id="ORD-3")
    valid, scorecard = _run(tmp_path, [json.dumps(GOOD), json.dumps(no_id_a), json.dumps(no_id_b)])

    assert list(valid["event_id"]) == ["evt-ok"]
    assert valid["event_id"].notna().all()
    not_null = scorecard[scorecard["check_name"] == "event_id_not_null"].iloc[0]
    # Both id-less rows are counted: deduplication no longer merges them into one.
    assert int(not_null["failed_record_count"]) == 2


def test_malformed_json_never_reaches_silver(tmp_path):
    valid, _ = _run(tmp_path, [json.dumps(GOOD), '{"event_id": "broken", "event_timestamp": '])

    assert list(valid["event_id"]) == ["evt-ok"]
    if "_malformed_record" in valid:
        assert valid["_malformed_record"].isna().all()
