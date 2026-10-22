import json
from collections import Counter

from confluent_kafka import Consumer

TOPIC = "medicines"


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "analytics-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    total_count = 0
    total_price = 0.0
    by_country = Counter()

    print("Analytics consumer started. Waiting for events... (Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            medicine = json.loads(msg.value().decode("utf-8"))

            total_count += 1
            total_price += medicine["price"]
            by_country[medicine["delivery_country"]] += 1

            print(f"---- Received: {medicine['name']} (offset={msg.offset()}) ----")
            print(f"Medicines received : {total_count}")
            print(f"Total price        : {total_price:.2f} EUR")
            print(f"Average price      : {total_price / total_count:.2f} EUR")
            print("By country         :")
            for country, count in by_country.items():
                print(f"   {country}: {count}")

    except KeyboardInterrupt:
        print("\nStopping analytics consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()