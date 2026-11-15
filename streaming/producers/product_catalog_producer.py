from streaming.producers.event_simulator import run_producer

if __name__ == "__main__":
    run_producer("product_catalog_updates", "localhost:9092", None, 1, 60, 5)
