TOPICS = {
    "customer_events": {
        "topic": "customer_events",
        "events": [
            "page_view",
            "product_view",
            "add_to_cart",
            "remove_from_cart",
            "checkout_started",
            "search",
            "logout",
        ],
    },
    "orders": {
        "topic": "orders",
        "events": ["order_created", "order_confirmed", "order_cancelled", "order_returned"],
    },
    "payments": {
        "topic": "payments",
        "events": [
            "payment_success",
            "payment_failed",
            "payment_pending",
            "refund_success",
            "refund_failed",
        ],
    },
    "inventory_updates": {
        "topic": "inventory_updates",
        "events": [
            "stock_added",
            "stock_reserved",
            "stock_released",
            "stockout_warning",
            "inventory_adjusted",
        ],
    },
    "product_catalog_updates": {
        "topic": "product_catalog_updates",
        "events": ["product_created", "product_updated", "price_changed", "category_changed", "product_deactivated"],
    },
    "shipment_events": {
        "topic": "shipment_events",
        "events": ["shipment_created", "shipped", "delayed", "delivered", "returned"],
    },
}

TOPIC_NAMES = [spec["topic"] for spec in TOPICS.values()]
