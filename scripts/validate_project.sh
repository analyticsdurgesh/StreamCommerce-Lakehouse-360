#!/usr/bin/env bash
set -euo pipefail

python -m compileall streaming databricks dashboard scripts
pytest -q
