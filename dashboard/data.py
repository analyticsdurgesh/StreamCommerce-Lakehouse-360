from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd


GOLD_PATH = Path(__import__("os").getenv("GOLD_DATA_PATH", "data/gold"))


def read_gold_table(name: str) -> pd.DataFrame:
    path = GOLD_PATH / name
    files = sorted(path.glob("*.parquet")) if path.exists() else []
    if files:
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return sample_table(name)


def sample_table(name: str) -> pd.DataFrame:
    days = [(date.today() - timedelta(days=i)).isoformat() for i in range(13, -1, -1)]
    if name == "daily_revenue":
        return pd.DataFrame({"event_date": days, "total_revenue": [8500 + i * 750 for i in range(14)], "total_orders": [120 + i * 6 for i in range(14)], "average_order_value": [72 + i for i in range(14)]})
    if name == "product_performance":
        return pd.DataFrame({"product_id": [f"SKU-{1000+i}" for i in range(10)], "category": ["electronics", "home", "fashion", "beauty", "sports"] * 2, "units_sold": [80, 65, 53, 49, 42, 39, 34, 31, 27, 22], "revenue": [18000, 12400, 9800, 7600, 6900, 6100, 5300, 4700, 3800, 3200]})
    if name == "customer_360":
        return pd.DataFrame({"customer_id": [f"CUST-{10000+i}" for i in range(20)], "lifetime_value": [120 + i * 45 for i in range(20)], "orders": [(i % 5) + 1 for i in range(20)], "sessions": [(i % 8) + 2 for i in range(20)], "repeat_purchase_flag": [i % 2 == 0 for i in range(20)]})
    if name == "inventory_health":
        return pd.DataFrame({"product_id": [f"SKU-{1000+i}" for i in range(10)], "stock_events": [20, 18, 16, 14, 12, 10, 9, 8, 7, 6], "stockout_warnings": [1, 0, 3, 2, 0, 4, 1, 0, 2, 3], "stockout_risk": [0.05, 0, 0.18, 0.14, 0, 0.4, 0.11, 0, 0.28, 0.5]})
    if name == "payment_reliability":
        return pd.DataFrame({"event_date": days, "payments": [140 + i for i in range(14)], "failed_payments": [5 + (i % 4) for i in range(14)], "failed_payment_rate": [0.03 + (i % 4) * 0.006 for i in range(14)]})
    if name == "data_quality_scorecard":
        return pd.DataFrame({"checked_at": days, "table_name": ["orders", "payments", "customer_events", "inventory_updates"] * 4, "check_name": ["event_id_unique", "payment_status_valid_values", "event_timestamp_not_null", "quantity_min_1"] * 4, "pass_percentage": [0.98, 0.94, 0.97, 0.91] * 4, "failed_record_count": [2, 9, 4, 12] * 4, "total_record_count": [100, 150, 130, 120] * 4, "passed": [True, False, True, False] * 4})
    return pd.DataFrame()
