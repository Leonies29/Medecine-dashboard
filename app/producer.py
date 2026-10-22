import json
from pathlib import Path

from confluent_kafka import Producer

TOPIC = "medicines"
DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "medicines.json"


def delivery_callback(err, msg):
    """Called once Kafka confirms (or fails) the delivery of a message."""
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}")
    else:
        print(
            f"Delivered key={msg.key().decode('utf-8')} "
            f"to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    with open(DATA_FILE, "r", encoding="utf-8") as file:
        medicines = json.load(file)

    print(f"{len(medicines)} medicines loaded from {DATA_FILE}")

    for medicine in medicines:
        value = json.dumps(medicine)
        key = str(medicine["id"])

        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            callback=delivery_callback,
        )
        print(f"Prepared: {medicine['name']} ({medicine['price']} EUR, {medicine['delivery_country']})")

        producer.poll(0)

    remaining = producer.flush(timeout=10)
    if remaining > 0:
        print(f"Warning: {remaining} message(s) were not delivered.")
    else:
        print("All messages delivered.")


if __name__ == "__main__":
    main()