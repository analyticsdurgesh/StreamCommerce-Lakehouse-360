from streaming.producers.event_simulator import run_producer

if __name__ == "__main__":
    run_producer("shipment_events", "localhost:9092", None, 2, 60, 6)
