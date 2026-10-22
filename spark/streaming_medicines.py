from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, round as spark_round, struct, sum as spark_sum, to_json
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_SERVERS = "kafka:29092"
INPUT_TOPIC = "medicines"
OUTPUT_TOPIC = "medicines-stats"

# Same structure as the messages sent by producer.py
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("delivery_country", StringType()),
])

spark = SparkSession.builder.appName("MedicinesStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read the Kafka stream
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 2. Parse the JSON value into columns
medicines = (
    raw.select(
        col("partition"),
        col("offset"),
        col("timestamp"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )
    .select("partition", "offset", "timestamp", "data.*")
    .filter(col("id").isNotNull())  # ignores messages that are not valid JSON
)

# 3. Query 1: display each order as it arrives (like consumer_pharmacy)
orders_query = (
    medicines.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .queryName("orders")
    .start()
)

# 4. Query 2: statistics by country (like consumer_analytics)
stats_by_country = medicines.groupBy("delivery_country").agg(
    count("*").alias("nb_orders"),
    spark_round(spark_sum("price"), 2).alias("total_eur"),
    spark_round(avg("price"), 2).alias("avg_price_eur"),
)

country_query = (
    stats_by_country.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .queryName("stats_by_country")
    .start()
)

# 5. Query 3: statistics by category
stats_by_category = medicines.groupBy("category").agg(
    count("*").alias("nb_orders"),
    spark_round(spark_sum("price"), 2).alias("total_eur"),
)

category_query = (
    stats_by_category.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .queryName("stats_by_category")
    .start()
)

# 6. Query 4: send the country stats back to Kafka
kafka_output = stats_by_country.select(
    col("delivery_country").alias("key"),
    to_json(struct("*")).alias("value"),
)

kafka_query = (
    kafka_output.writeStream.outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/medicines-stats")
    .queryName("stats_to_kafka")
    .start()
)

spark.streams.awaitAnyTermination()