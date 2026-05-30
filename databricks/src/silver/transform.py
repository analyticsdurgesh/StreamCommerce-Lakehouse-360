from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from databricks.src.quality.checks import ecommerce_checks
from databricks.src.utils.io import read_table, write_table


def parse_bronze(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        try:
            payload = json.loads(row.get("raw_json") or "{}")
        except json.JSONDecodeError:
            payload = {"_malformed_record": row.get("raw_json")}
        payload.update(
            {
                "ingestion_timestamp": row.get("ingestion_timestamp"),
                "source_topic": row.get("source_topic"),
                "source_file": row.get("source_file"),
                "batch_id": row.get("batch_id"),
                "processing_date": row.get("processing_date"),
            }
        )
        rows.append(payload)
    return pd.DataFrame(rows)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    normalized = df.copy()
    if "event_timestamp" in normalized:
        normalized["event_timestamp"] = pd.to_datetime(normalized["event_timestamp"], errors="coerce", utc=True)
        normalized["event_date"] = normalized["event_timestamp"].dt.date.astype("string")
    for col in ["price", "amount"]:
        if col in normalized:
            normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    if "quantity" in normalized:
        normalized["quantity"] = pd.to_numeric(normalized["quantity"], errors="coerce").astype("Int64")
    if "payment_status" in normalized:
        status_map = {
            "payment_success": "SUCCESS",
            "payment_failed": "FAILED",
            "payment_pending": "PENDING",
            "refund_success": "REFUNDED",
        }
        normalized["payment_status"] = normalized["payment_status"].fillna(normalized.get("event_type").map(status_map))
        normalized["payment_status"] = normalized["payment_status"].astype("string").str.upper()
    if "event_id" in normalized and "event_timestamp" in normalized:
        normalized = normalized.sort_values("event_timestamp").drop_duplicates("event_id", keep="last")
    normalized["dq_is_valid"] = True
    return normalized


def transform_topic(bronze_root: str, silver_root: str, quarantine_root: str, topic: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    bronze_path = Path(bronze_root) / topic
    parts = []
    for partition in bronze_path.glob("processing_date=*"):
        parts.append(read_table(partition))
    bronze_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    parsed = normalize(parse_bronze(bronze_df))
    runner = ecommerce_checks(topic, parsed)
    scorecard = runner.scorecard()
    quarantine = runner.quarantine()
    invalid_ids = set(quarantine["event_id"].dropna()) if "event_id" in quarantine else set()
    valid = parsed[~parsed["event_id"].isin(invalid_ids)].copy() if "event_id" in parsed else parsed
    write_table(valid, Path(silver_root) / topic)
    if not quarantine.empty:
        write_table(quarantine, Path(quarantine_root) / "dq_failures" / topic)
    write_table(scorecard, Path(silver_root) / "_dq_results" / topic)
    return valid, scorecard


def transform_all(bronze_root: str = "data/bronze", silver_root: str = "data/silver", quarantine_root: str = "data/quarantine") -> dict[str, int]:
    topics = [p.name for p in Path(bronze_root).iterdir() if p.is_dir()] if Path(bronze_root).exists() else []
    counts = {}
    for topic in topics:
        valid, _ = transform_topic(bronze_root, silver_root, quarantine_root, topic)
        counts[topic] = len(valid)
    return counts
