#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="transactions_ibm")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--hdfs-dir", default="/data/ibm_transactions_processed")
    parser.add_argument("--batch-duration", type=int, default=30)
    args = parser.parse_args()
    
    # Create Spark session with Kafka package
    spark = SparkSession.builder \
        .appName("IBM_AML_Streaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()
    
    # Define schema
    schema = StructType([
        StructField("Timestamp", StringType(), True),
        StructField("From Bank", IntegerType(), True),
        StructField("Account", StringType(), True),
        StructField("To Bank", IntegerType(), True),
        StructField("Account.1", StringType(), True),
        StructField("Amount Received", DoubleType(), True),
        StructField("Receiving Currency", StringType(), True),
        StructField("Amount Paid", DoubleType(), True),
        StructField("Payment Currency", StringType(), True),
        StructField("Payment Format", StringType(), True),
        StructField("Is Laundering", IntegerType(), True),
        StructField("kafka_timestamp", StringType(), True),
    ])
    
    print("="*60)
    print("🚀 SPARK STREAMING CONSUMER - IBM AML")
    print("="*60)
    print(f"📡 Reading from Kafka topic: {args.topic}")
    print(f"📤 Writing to HDFS: {args.hdfs_dir}")
    print(f"⏱️ Batch duration: {args.batch_duration} seconds")
    print("="*60 + "\n")
    
    # Read from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and apply schema
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("Timestamp").isNotNull()) \
        .withColumn("processing_time", current_timestamp())
    
    # Enrich the data
    enriched_stream = parsed_stream \
        .withColumn("amount_category",
                   when(col("Amount Received") > 10000, "HIGH")
                   .when(col("Amount Received") > 1000, "MEDIUM")
                   .otherwise("LOW")) \
        .withColumn("event_timestamp", to_timestamp(col("Timestamp"), "yyyy/MM/dd HH:mm")) \
        .withColumn("hour_of_day", hour(col("event_timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("event_timestamp"))) \
        .withColumn("is_laundering", when(col("Is Laundering") == 1, "Yes").otherwise("No"))
    
    # Write to HDFS
    def write_to_hdfs(df, epoch_id):
        output_path = args.hdfs_dir
        df.write \
            .mode("append") \
            .partitionBy("day_of_week", "amount_category") \
            .parquet(output_path)
        print(f"📦 Batch {epoch_id}: Wrote {df.count()} records to HDFS")
    
    # Start streaming query
    query = enriched_stream.writeStream \
        .foreachBatch(write_to_hdfs) \
        .trigger(processingTime=f"{args.batch_duration} seconds") \
        .option("checkpointLocation", f"{args.hdfs_dir}/_checkpoints") \
        .start()
    
    print("✅ Streaming started! Press Ctrl+C to stop\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()
