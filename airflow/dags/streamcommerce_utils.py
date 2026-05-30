from __future__ import annotations

import os
import subprocess
from pathlib import Path


def project_root() -> Path:
    return Path(os.getenv("STREAMCOMMERCE_PROJECT_ROOT", "/opt/airflow"))


def run_local_command(args: list[str]) -> str:
    proc = subprocess.run(args, cwd=project_root(), text=True, capture_output=True, check=True)
    return proc.stdout


def databricks_or_local_task(notebook_task_key: str, local_args: list[str]) -> str:
    if os.getenv("ENVIRONMENT", "local") == "local":
        return run_local_command(local_args)
    return f"Databricks task {notebook_task_key} should be submitted by DatabricksSubmitRunOperator in prod."
