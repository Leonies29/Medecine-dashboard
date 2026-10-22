import json

from confluent_kafka import Consumer

TOPIC = "medicines-stats"


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "stats-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print("Stats consumer started (Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            stats = json.loads(msg.value().decode("utf-8"))
            print(
                f"[Spark update] {stats['delivery_country']:<8} | "
                f"orders={stats['nb_orders']:<4} | "
                f"total={stats['total_eur']:>8.2f} EUR | "
                f"avg={stats['avg_price_eur']:.2f} EUR"
            )
    except KeyboardInterrupt:
        print("\nStopping stats consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()