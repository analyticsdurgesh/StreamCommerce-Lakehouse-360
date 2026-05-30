#!/usr/bin/env bash
set -euo pipefail

python scripts/seed_reference_data.py
python -m databricks.src.local_pipeline
