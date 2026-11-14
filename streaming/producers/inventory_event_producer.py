from streaming.producers.event_simulator import run_producer

if __name__ == "__main__":
    run_producer("inventory_updates", "localhost:9092", None, 2, 60, 8)
