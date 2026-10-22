import json
import random
import time
from pathlib import Path

from confluent_kafka import Producer

TOPIC = "medicines"
DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "medicines.json"
COUNTRIES = ["France","Portugal","Belgium", "Spain", "Germany", "Italy"]


def delivery_callback(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"  -> partition={msg.partition()} offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    with open(DATA_FILE, "r", encoding="utf-8") as file:
        catalog = json.load(file)

    print("Streaming producer started (Ctrl+C to stop)")

    try:
        while True:
            medicine = dict(random.choice(catalog))
            medicine["delivery_country"] = random.choice(COUNTRIES)

            producer.produce(
                topic=TOPIC,
                key=str(medicine["id"]),
                value=json.dumps(medicine),
                callback=delivery_callback,
            )
            print(f"Sent: {medicine['name']} -> {medicine['delivery_country']} ({medicine['price']} EUR)")

            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()