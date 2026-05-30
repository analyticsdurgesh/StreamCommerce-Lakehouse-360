from __future__ import annotations

from pathlib import Path

import pandas as pd

from databricks.src.utils.io import read_table, write_table


def _load(root: str, table: str) -> pd.DataFrame:
    df = read_table(Path(root) / table)
    if "event_timestamp" in df:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)
        df["event_date"] = df["event_timestamp"].dt.date.astype("string")
    return df


def build_daily_revenue(silver_root: str) -> pd.DataFrame:
    orders = _load(silver_root, "orders")
    payments = _load(silver_root, "payments")
    if orders.empty:
        return pd.DataFrame(columns=["event_date", "total_revenue", "total_orders", "average_order_value"])
    paid_orders = orders.copy()
    if not payments.empty and {"order_id", "payment_status"}.issubset(payments.columns):
        paid = payments[payments["payment_status"].eq("SUCCESS")][["order_id"]].drop_duplicates()
        paid_orders = orders.merge(paid, on="order_id", how="inner")
    revenue = pd.to_numeric(paid_orders.get("amount", paid_orders.get("price", 0)), errors="coerce").fillna(0)
    paid_orders = paid_orders.assign(revenue=revenue)
    grouped = paid_orders.groupby("event_date", dropna=False).agg(total_revenue=("revenue", "sum"), total_orders=("order_id", "nunique")).reset_index()
    grouped["average_order_value"] = grouped["total_revenue"] / grouped["total_orders"].replace(0, pd.NA)
    return grouped


def build_product_performance(silver_root: str) -> pd.DataFrame:
    orders = _load(silver_root, "orders")
    if orders.empty:
        return pd.DataFrame(columns=["product_id", "category", "units_sold", "revenue"])
    orders["revenue"] = pd.to_numeric(orders.get("amount", orders.get("price", 0)), errors="coerce").fillna(0)
    return orders.groupby(["product_id", "category"], dropna=False).agg(units_sold=("quantity", "sum"), revenue=("revenue", "sum"), orders=("order_id", "nunique")).reset_index()


def build_customer_360(silver_root: str) -> pd.DataFrame:
    orders = _load(silver_root, "orders")
    events = _load(silver_root, "customer_events")
    if orders.empty and events.empty:
        return pd.DataFrame(columns=["customer_id", "lifetime_value", "orders", "sessions", "repeat_purchase_flag"])
    order_metrics = pd.DataFrame()
    if not orders.empty:
        orders["revenue"] = pd.to_numeric(orders.get("amount", orders.get("price", 0)), errors="coerce").fillna(0)
        order_metrics = orders.groupby("customer_id", dropna=False).agg(lifetime_value=("revenue", "sum"), orders=("order_id", "nunique")).reset_index()
    session_metrics = pd.DataFrame()
    if not events.empty:
        session_metrics = events.groupby("customer_id", dropna=False).agg(sessions=("session_id", "nunique")).reset_index()
    if not order_metrics.empty and not session_metrics.empty:
        out = order_metrics.merge(session_metrics, on="customer_id", how="outer")
    elif not order_metrics.empty:
        out = order_metrics
        out["sessions"] = 0
    else:
        out = session_metrics
        out["lifetime_value"] = 0
        out["orders"] = 0
    out["repeat_purchase_flag"] = out.get("orders", 0).fillna(0) > 1
    return out.fillna({"lifetime_value": 0, "orders": 0, "sessions": 0})


def build_inventory_health(silver_root: str) -> pd.DataFrame:
    inv = _load(silver_root, "inventory_updates")
    if inv.empty:
        return pd.DataFrame(columns=["product_id", "stock_events", "stockout_warnings", "stockout_risk"])
    grouped = inv.groupby("product_id", dropna=False).agg(
        stock_events=("event_id", "count"),
        stockout_warnings=("event_type", lambda s: s.isin(["stockout_warning"]).sum()),
    ).reset_index()
    grouped["stockout_risk"] = grouped["stockout_warnings"] / grouped["stock_events"].replace(0, pd.NA)
    return grouped


def build_payment_reliability(silver_root: str) -> pd.DataFrame:
    payments = _load(silver_root, "payments")
    if payments.empty:
        return pd.DataFrame(columns=["event_date", "payments", "failed_payments", "failed_payment_rate"])
    grouped = payments.groupby("event_date", dropna=False).agg(
        payments=("payment_id", "nunique"),
        failed_payments=("payment_status", lambda s: s.eq("FAILED").sum()),
    ).reset_index()
    grouped["failed_payment_rate"] = grouped["failed_payments"] / grouped["payments"].replace(0, pd.NA)
    return grouped


def build_data_quality_scorecard(silver_root: str) -> pd.DataFrame:
    root = Path(silver_root) / "_dq_results"
    frames = [read_table(p) for p in root.iterdir() if p.is_dir()] if root.exists() else []
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_all(silver_root: str = "data/silver", gold_root: str = "data/gold") -> dict[str, int]:
    builders = {
        "daily_revenue": build_daily_revenue,
        "product_performance": build_product_performance,
        "customer_360": build_customer_360,
        "inventory_health": build_inventory_health,
        "payment_reliability": build_payment_reliability,
        "data_quality_scorecard": build_data_quality_scorecard,
    }
    counts = {}
    for name, builder in builders.items():
        df = builder(silver_root)
        write_table(df, Path(gold_root) / name)
        counts[name] = len(df)
    return counts
