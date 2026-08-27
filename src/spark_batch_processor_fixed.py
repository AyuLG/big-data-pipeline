#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import os

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ibm_aml_data.csv")
    parser.add_argument("--output-dir", default="local_output")
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("IBM_AML_Batch") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("="*60)
    print("🚀 SPARK BATCH PROCESSOR - IBM AML")
    print("="*60)
    
    # Read CSV
    print(f"📂 Reading CSV: {args.csv}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.csv)
    total = df.count()
    print(f"📊 Loaded {total:,} rows")
    
    # Show schema
    print("\n📋 Schema:")
    df.printSchema()
    
    # Enrich data
    enriched = df \
        .withColumn("amount_category",
                   when(col("Amount Received") > 10000, "HIGH")
                   .when(col("Amount Received") > 1000, "MEDIUM")
                   .otherwise("LOW")) \
        .withColumn("event_timestamp", to_timestamp(col("Timestamp"), "yyyy/MM/dd HH:mm")) \
        .withColumn("hour_of_day", hour(col("event_timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("event_timestamp"))) \
        .withColumn("is_laundering", when(col("Is Laundering") == 1, "Yes").otherwise("No"))
    
    # Show sample
    print("\n🔍 Sample data (5 rows):")
    enriched.show(5, truncate=False)
    
    # Show statistics
    print("\n📊 Laundering Statistics:")
    enriched.groupBy("is_laundering").count().show()
    
    print("\n💰 Transaction Amount by Category:")
    enriched.groupBy("amount_category").agg(
        count("*").alias("count"),
        avg("Amount Received").alias("avg_amount"),
        sum("Amount Received").alias("total_amount")
    ).show()
    
    # Save locally
    print(f"\n📤 Saving to: {args.output_dir}")
    enriched.write.mode("overwrite").parquet(args.output_dir)
    print(f"✅ Data saved to: {args.output_dir}")
    
    spark.stop()

if __name__ == "__main__":
    main()
