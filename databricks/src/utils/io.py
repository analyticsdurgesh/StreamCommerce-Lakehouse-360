from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_jsonl(records: Iterable[dict], path: str | Path) -> Path:
    p = Path(path)
    ensure_dir(p.parent)
    with p.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, default=str) + "\n")
    return p


def read_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    rows: list[dict] = []
    if not p.exists():
        return rows
    for line in p.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"_malformed_record": line})
    return rows


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    p = ensure_dir(path)
    output = p / "part-00000.parquet"
    if df.empty:
        df.to_csv(p / "_EMPTY.csv", index=False)
    else:
        try:
            df.to_parquet(output, index=False)
        except ImportError:
            output = p / "part-00000.csv"
            df.to_csv(output, index=False)
    return output


def read_table(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    files = sorted(p.glob("*.parquet"))
    if not files:
        csv_files = [f for f in sorted(p.glob("*.csv")) if f.name != "_EMPTY.csv"]
        return pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True) if csv_files else pd.DataFrame()
    try:
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    except ImportError:
        csv_files = [f for f in sorted(p.glob("*.csv")) if f.name != "_EMPTY.csv"]
        return pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True) if csv_files else pd.DataFrame()
