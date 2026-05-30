import pandas as pd

from databricks.src.silver.transform import normalize


def test_normalize_deduplicates_by_event_id_latest_timestamp():
    df = pd.DataFrame(
        [
            {"event_id": "1", "event_timestamp": "2026-05-30T00:00:00Z", "event_type": "order_created", "price": "10", "quantity": "1"},
            {"event_id": "1", "event_timestamp": "2026-05-30T01:00:00Z", "event_type": "order_confirmed", "price": "12", "quantity": "2"},
        ]
    )
    out = normalize(df)
    assert len(out) == 1
    assert out.iloc[0]["event_type"] == "order_confirmed"
