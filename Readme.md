# 💊 Medicine Orders: Real-Time Streaming Pipeline

A real-time data streaming project built with **Apache Kafka**, **Apache Spark Structured Streaming**, **Python** and **Streamlit**.

A fictional pharmaceutical company streams medicine orders from several European countries. The orders flow through Kafka, are aggregated in real time by Spark, and are displayed on an interactive web dashboard with filters.

> This is a technical exercise using fictional data. It does not cover prescriptions, dosages or patient care.

---

## 📐 Architecture

```
producer.py / producer_stream.py ──▶ topic "medicines" ──▶ Spark Streaming ──▶ topic "medicines-stats" ──▶ Streamlit dashboard
kafka-console-producer.sh ─────────┘          │
                                              ▼
                          consumer_pharmacy.py / consumer_analytics.py
```

- **Kafka** (KRaft mode, no ZooKeeper) is the central message hub.
- **Python producers** publish medicine orders as JSON messages to the `medicines` topic.
- **Python consumers** read the same topic in two independent consumer groups (`pharmacy-group` and `analytics-group`), so each one receives every message.
- **Spark Structured Streaming** reads the `medicines` topic, parses the JSON with a schema, computes statistics per country and per category, and publishes the country statistics to the `medicines-stats` topic.
- **Streamlit** displays a live dashboard with KPIs, charts and filters.

---

## 🛠️ Tech stack

| Component | Version |
|---|---|
| Apache Kafka (Docker) | 4.1.1 |
| Apache Spark (Docker) | 3.5.0 |
| Python | 3.10+ |
| confluent-kafka | ≥ 2.6, < 3.0 |
| Streamlit | ≥ 1.37 |
| pandas | latest |

---

## 📁 Project structure

```
tp-kafka-medicines/
├── docker-compose.yml          # Kafka (KRaft) + Spark containers
├── requirements.txt            # Python dependencies
├── data/
│   └── medicines.json          # Initial dataset (5 medicines)
├── app/
│   ├── producer.py             # Sends the 5 medicines once
│   ├── producer_stream.py      # Sends a random order every second
│   ├── consumer_pharmacy.py    # Consumer 1: displays each order
│   ├── consumer_analytics.py   # Consumer 2: computes statistics
│   ├── consumer_stats.py       # Reads Spark results from Kafka
│   └── dashboard.py            # Streamlit web dashboard
└── spark/
    └── streaming_medicines.py  # Spark Structured Streaming job
```

---

## 📦 Data format

Each message sent to Kafka is a JSON object:

```json
{
  "id": 101,
  "name": "Paracetamol",
  "category": "Pain Relief",
  "price": 4.5,
  "delivery_country": "France"
}
```

Prices are fictional and expressed in euros.

---

## ✅ Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Python 3.10 or later
- An internet connection for the first Spark run (it downloads the Kafka connector)

---

## 🚀 Getting started

### 1. Start Kafka and Spark

```powershell
docker compose up -d
docker compose ps
```

Both `kafka-medicines` and `spark-medicines` should be `Up`.

### 2. Create the topics

```powershell
docker exec kafka-medicines /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic medicines --partitions 3 --replication-factor 1
docker exec kafka-medicines /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic medicines-stats --partitions 1 --replication-factor 1
```

### 3. Set up Python

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 4. Run the pipeline (3 terminals)

**Terminal 1: Spark job**

```powershell
docker exec -it spark-medicines /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/streaming_medicines.py
```

**Terminal 2: Dashboard**

```powershell
python -m streamlit run app/dashboard.py
```

Then open [http://localhost:8501](http://localhost:8501).

**Terminal 3: Order generator**

```powershell
python app/producer_stream.py
```

### 5. (Optional) Send an order by hand

```powershell
docker exec -it kafka-medicines /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic medicines
```

Then type a JSON line, for example:

```
{"id": 106, "name": "Aspirin", "category": "Pain Relief", "price": 3.90, "delivery_country": "Portugal"}
```

---

## 📊 Dashboard features

- **Live KPIs**: number of orders, total revenue, average price, number of countries
- **Charts**: orders by country, revenue by category, orders over time, orders by Kafka partition
- **Top medicines** table and **latest orders** feed
- **Spark statistics** section showing the results computed by Spark
- **Filters**: country, category, medicine, name search, price range, time window, Kafka partition

The charts are computed from the raw orders, so every filter applies to them. Spark's statistics are already aggregated per country, so only the country filter applies to that section.

---

## 🔍 Key concepts demonstrated

- **Topics and partitions**: the `medicines` topic has 3 partitions; messages with the same key (medicine `id`) always go to the same partition.
- **Consumer groups**: consumers in different groups each receive all messages, with separate offsets. Consumers in the same group share the partitions.
- **Message retention**: Kafka keeps messages after they are read; a new consumer group can re-read the topic from the beginning.
- **Micro-batch processing**: Spark processes the stream in small batches rather than message by message.
- **Output modes**: `append` (new rows only), `complete` (full table each time) and `update` (changed rows only).
- **Chained pipeline**: Spark is both a consumer (`medicines`) and a producer (`medicines-stats`).

---

## 🧯 Troubleshooting

| Problem | Solution |
|---|---|
| Kafka container stops a few seconds after starting (`UnknownHostException`) | Make sure `hostname: kafka` and the explicit `kafka:` listeners are in `docker-compose.yml`, then run `docker compose down -v` and `docker compose up -d`. |
| Python scripts can't connect to Kafka | Check that `docker compose ps` shows `0.0.0.0:9092->9092/tcp`. |
| Spark refuses to start after a code change | Delete the old checkpoint: `docker exec spark-medicines rm -rf /tmp/checkpoints` |
| Dashboard says "Waiting for Spark statistics" | Check that the Spark job is still running in Terminal 1. |
| `Activate.ps1` is blocked | Run `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned` |

---

## ⚠️ Limitations

This setup uses a single Kafka broker with a replication factor of 1. It is suitable for local development and learning, but provides neither the security nor the high availability required in production.

---

## 👤 Author

**Your Name** · Engineering school, Year 3 · Real-Time Data Streaming course
