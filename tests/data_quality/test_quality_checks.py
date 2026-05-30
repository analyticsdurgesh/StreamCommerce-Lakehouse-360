import pandas as pd

from databricks.src.quality.checks import ecommerce_checks


def test_quality_runner_flags_invalid_payment_status_and_negative_price():
    df = pd.DataFrame(
        [
            {"event_id": "1", "event_timestamp": "2026-05-30T00:00:00Z", "event_type": "payment_success", "price": 10, "quantity": 1, "payment_status": "SUCCESS", "amount": 10},
            {"event_id": "2", "event_timestamp": None, "event_type": "payment_success", "price": -1, "quantity": 0, "payment_status": "NOPE", "amount": -1},
        ]
    )
    runner = ecommerce_checks("payments", df)
    scorecard = runner.scorecard()
    quarantine = runner.quarantine()
    assert (scorecard["failed_record_count"] > 0).any()
    assert not quarantine.empty
    assert {"check_name", "reason_code"}.issubset(quarantine.columns)
