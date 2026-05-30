from kafka.producers.event_simulator import run_producer

if __name__ == "__main__":
    run_producer("customer_events", "localhost:9092", None, 5, 60, 8)
