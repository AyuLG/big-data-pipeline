#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("AML_Summary").getOrCreate()
    
    print("="*70)
    print("📊 IBM AML DATASET - COMPLETE SUMMARY")
    print("="*70)
    
    # Read the processed data
    df = spark.read.parquet("ibm_aml_processed")
    total = df.count()
    
    print(f"\n📈 TOTAL TRANSACTIONS: {total:,}")
    
    # Laundering stats
    laundering_stats = df.groupBy("is_laundering").count().collect()
    print("\n🚨 LAUNDERING STATISTICS:")
    for row in laundering_stats:
        pct = (row["count"] / total) * 100
        print(f"  {row['is_laundering']}: {row['count']:,} ({pct:.2f}%)")
    
    # Amount stats
    print("\n💰 TRANSACTION AMOUNT STATISTICS:")
    df.select("Amount Received").describe().show()
    
    # Category breakdown
    print("\n📊 AMOUNT CATEGORY BREAKDOWN:")
    df.groupBy("amount_category").agg(
        count("*").alias("count"),
        avg("Amount Received").alias("avg_amount"),
        sum("Amount Received").alias("total_amount")
    ).show()
    
    # Day of week breakdown
    print("\n📅 TRANSACTIONS BY DAY OF WEEK:")
    df.groupBy("day_of_week").agg(
        count("*").alias("count"),
        sum("Amount Received").alias("total_amount")
    ).orderBy("day_of_week").show()
    
    # Hour of day breakdown
    print("\n🕐 TRANSACTIONS BY HOUR OF DAY:")
    df.groupBy("hour_of_day").agg(
        count("*").alias("count")
    ).orderBy("hour_of_day").show(24)
    
    spark.stop()

if __name__ == "__main__":
    main()
