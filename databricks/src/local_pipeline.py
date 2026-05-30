from __future__ import annotations

from databricks.src.bronze.ingest import ingest_all
from databricks.src.gold.aggregate import build_all
from databricks.src.silver.transform import transform_all


def run_local_pipeline() -> dict:
    return {
        "bronze": ingest_all(),
        "silver": transform_all(),
        "gold": build_all(),
    }


if __name__ == "__main__":
    print(run_local_pipeline())
