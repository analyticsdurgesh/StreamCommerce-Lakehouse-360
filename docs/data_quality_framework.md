# Data Quality Framework

The framework in `databricks/src/quality/checks.py` is self-contained and emits one row per check with:

- `passed`
- `failed_record_count`
- `total_record_count`
- `pass_percentage`
- `severity`
- `reason_code`
- `checked_at`

Invalid records are copied to quarantine with `check_name` and `reason_code`. Critical checks can fail the Airflow DQ DAG when pass percentage falls below the configured threshold.

Implemented categories: schema, completeness, validity, uniqueness, freshness-ready scorecarding, and business rules.
