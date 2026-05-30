from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from kafka.topics import TOPIC_NAMES

from databricks.src.utils.io import ensure_dir, read_jsonl, utc_now_iso, write_table


def ingest_topic_to_bronze(landing_root: str, bronze_root: str, topic: str, processing_date: str | None = None) -> pd.DataFrame:
    processing_date = processing_date or date.today().isoformat()
    source_dir = Path(landing_root) / topic
    files = sorted(source_dir.glob("**/*.jsonl"))
    rows: list[dict] = []
    batch_id = str(uuid4())
    for file in files:
        for payload in read_jsonl(file):
            rows.append(
                {
                    "raw_json": json.dumps(payload),
                    "event_id": payload.get("event_id"),
                    "ingestion_timestamp": utc_now_iso(),
                    "source_topic": topic,
                    "source_file": str(file),
                    "batch_id": batch_id,
                    "processing_date": processing_date,
                    "_malformed_record": payload.get("_malformed_record"),
                }
            )
    df = pd.DataFrame(rows)
    write_table(df, Path(bronze_root) / topic / f"processing_date={processing_date}")
    return df


def ingest_all(landing_root: str = "data/landing", bronze_root: str = "data/bronze", topics: list[str] | None = None) -> dict[str, int]:
    topics = topics or [p.name for p in ensure_dir(landing_root).iterdir() if p.is_dir() and p.name in TOPIC_NAMES]
    return {topic: len(ingest_topic_to_bronze(landing_root, bronze_root, topic)) for topic in topics}
