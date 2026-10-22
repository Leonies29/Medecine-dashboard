import json

from confluent_kafka import Consumer

TOPIC = "medicines"


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "pharmacy-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print("Pharmacy consumer started. Waiting for orders... (Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            medicine = json.loads(msg.value().decode("utf-8"))

            print("---- New order ----")
            print(f"ID       : {medicine['id']}")
            print(f"Name     : {medicine['name']}")
            print(f"Category : {medicine['category']}")
            print(f"Price    : {medicine['price']:.2f} EUR")
            print(f"Country  : {medicine['delivery_country']}")
            print(f"(partition={msg.partition()}, offset={msg.offset()})")

    except KeyboardInterrupt:
        print("\nStopping pharmacy consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()