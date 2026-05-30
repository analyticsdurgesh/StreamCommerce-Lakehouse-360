#!/usr/bin/env bash
set -euo pipefail

python -m compileall kafka databricks dashboard scripts
pytest -q
