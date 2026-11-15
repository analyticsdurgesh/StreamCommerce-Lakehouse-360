from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import pandas as pd


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    passed: bool
    failed_record_count: int
    total_record_count: int
    pass_percentage: float
    severity: str
    reason_code: str

    def as_dict(self) -> dict:
        return {
            **self.__dict__,
            "checked_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }


class DataQualityRunner:
    def __init__(self, table_name: str, df: pd.DataFrame):
        self.table_name = table_name
        self.df = df.copy()
        self.failed_records: list[pd.DataFrame] = []
        self.results: list[CheckResult] = []

    def _record(self, check_name: str, mask: pd.Series, severity: str, reason_code: str) -> None:
        total = len(self.df)
        failed = int((~mask.fillna(False)).sum()) if total else 0
        passed = failed == 0
        pass_percentage = 1.0 if total == 0 else round((total - failed) / total, 4)
        self.results.append(CheckResult(check_name, self.table_name, passed, failed, total, pass_percentage, severity, reason_code))
        if failed:
            bad = self.df.loc[~mask.fillna(False)].copy()
            bad["check_name"] = check_name
            bad["reason_code"] = reason_code
            self.failed_records.append(bad)

    def required_columns(self, columns: list[str], severity: str = "critical") -> "DataQualityRunner":
        missing = [c for c in columns if c not in self.df.columns]
        mask = pd.Series([not missing] * len(self.df), index=self.df.index)
        self._record("required_columns", mask, severity, f"missing_columns:{','.join(missing)}" if missing else "ok")
        return self

    def not_null(self, column: str, severity: str = "critical") -> "DataQualityRunner":
        mask = self.df[column].notna() if column in self.df else pd.Series([False] * len(self.df), index=self.df.index)
        self._record(f"{column}_not_null", mask, severity, f"{column}_null")
        return self

    def valid_values(self, column: str, allowed: set[str], severity: str = "high") -> "DataQualityRunner":
        mask = self.df[column].isin(allowed) if column in self.df else pd.Series([False] * len(self.df), index=self.df.index)
        self._record(f"{column}_valid_values", mask, severity, f"{column}_invalid")
        return self

    def numeric_min(self, column: str, minimum: float, inclusive: bool = True, severity: str = "high") -> "DataQualityRunner":
        values = pd.to_numeric(self.df[column], errors="coerce") if column in self.df else pd.Series([None] * len(self.df), index=self.df.index)
        mask = values.ge(minimum) if inclusive else values.gt(minimum)
        self._record(f"{column}_min_{minimum}", mask, severity, f"{column}_below_min")
        return self

    def unique(self, column: str, severity: str = "medium") -> "DataQualityRunner":
        mask = ~self.df[column].duplicated(keep=False) if column in self.df else pd.Series([False] * len(self.df), index=self.df.index)
        self._record(f"{column}_unique", mask, severity, f"{column}_duplicate")
        return self

    def custom(self, check_name: str, predicate: Callable[[pd.DataFrame], pd.Series], reason_code: str, severity: str = "medium") -> "DataQualityRunner":
        self._record(check_name, predicate(self.df), severity, reason_code)
        return self

    def scorecard(self) -> pd.DataFrame:
        return pd.DataFrame([r.as_dict() for r in self.results])

    def quarantine(self) -> pd.DataFrame:
        return pd.concat(self.failed_records, ignore_index=True) if self.failed_records else pd.DataFrame()

    def failed_index(self) -> pd.Index:
        """Row labels (of the DataFrame the runner checked) that failed at least one check.

        Use this to split valid rows from bad ones. Splitting by event_id instead
        lets through exactly the rows that most need stopping: a row whose
        event_id is null, or a malformed record that has no event_id at all,
        matches no id and so is never removed.
        """
        labels: set = set()
        for failed in self.failed_records:
            labels.update(failed.index)
        return pd.Index(list(labels))


def ecommerce_checks(table_name: str, df: pd.DataFrame) -> DataQualityRunner:
    valid_payment_statuses = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
    valid_event_types = {
        "page_view",
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "search",
        "logout",
        "order_created",
        "order_confirmed",
        "order_cancelled",
        "order_returned",
        "payment_success",
        "payment_failed",
        "payment_pending",
        "refund_success",
        "refund_failed",
        "stock_added",
        "stock_reserved",
        "stock_released",
        "stockout_warning",
        "inventory_adjusted",
        "product_created",
        "product_updated",
        "price_changed",
        "category_changed",
        "product_deactivated",
        "shipment_created",
        "shipped",
        "delayed",
        "delivered",
        "returned",
    }
    runner = DataQualityRunner(table_name, df)
    runner.required_columns(["event_id", "event_timestamp", "event_type"])
    for column in ["event_id", "event_timestamp"]:
        runner.not_null(column)
    if "event_id" in df:
        runner.unique("event_id")
    if "price" in df:
        runner.numeric_min("price", 0)
    if "quantity" in df:
        runner.numeric_min("quantity", 1)
    if "payment_status" in df:
        runner.valid_values("payment_status", valid_payment_statuses)
    if "event_type" in df:
        runner.valid_values("event_type", valid_event_types)
    if "amount" in df and "payment_status" in df:
        runner.custom(
            "successful_payments_positive_amount",
            lambda d: (d["payment_status"] != "SUCCESS") | (pd.to_numeric(d["amount"], errors="coerce") > 0),
            "successful_payment_non_positive_amount",
            "critical",
        )
    return runner
