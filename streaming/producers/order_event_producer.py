from streaming.producers.event_simulator import run_producer

if __name__ == "__main__":
    run_producer("orders", "localhost:9092", None, 3, 60, 8)
