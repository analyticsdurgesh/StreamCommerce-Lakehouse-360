from kafka.producers.event_simulator import generate_event


def test_generate_order_event_has_required_shape():
    event = generate_event("orders", bad_data_percentage=0)
    assert event["event_id"]
    assert event["event_timestamp"]
    assert event["event_type"] in {"order_created", "order_confirmed", "order_cancelled", "order_returned"}
    assert event["price"] >= 0
    assert event["quantity"] > 0


def test_bad_data_injection_can_create_malformed_or_flagged_records():
    event = generate_event("payments", bad_data_percentage=100)
    assert isinstance(event, str) or "_defect_type" in event
