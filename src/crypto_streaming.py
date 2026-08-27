#!/usr/bin/env python3
# ============================================
# EXERCISE 8: Structured Streaming from Kafka
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("=" * 60)
print("⚡ EXERCISE 8: Structured Streaming from Kafka")
print("=" * 60)

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Crypto-Streaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

print(f"✅ Spark session created: {spark.version}")

# 2. Define Schema for JSON messages
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("coin", StringType(), True),
    StructField("trader_id", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

print("📋 Schema defined for streaming data")

# 3. Read from Kafka
print("\n📡 Reading from Kafka topic: crypto-practice")
print("⏳ Waiting for messages...")

streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-practice") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON messages
parsed_df = streaming_df \
    .select(from_json(col("value").cast("string"), trade_schema).alias("data")) \
    .select("data.*") \
    .filter(col("trade_id").isNotNull()) \
    .withColumn("event_timestamp", to_timestamp(col("event_time"))) \
    .withWatermark("event_timestamp", "2 minutes")

print("✅ JSON parsing configured")

# 5. Windowed aggregation (1-minute windows)
windowed_agg = parsed_df \
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("exchange")
    ) \
    .agg(
        count("*").alias("trade_count"),
        sum("amount_usd").alias("total_volume")
    ) \
    .orderBy("window")

print("✅ Windowed aggregation configured")

# 6. Write to console
query = windowed_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

print("\n✅ Streaming started! Press Ctrl+C to stop")
print("📊 Sending output to console every 10 seconds")
print("=" * 60)

# 7. Keep the stream running
query.awaitTermination()
