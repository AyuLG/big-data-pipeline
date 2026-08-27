#!/usr/bin/env python3
# ============================================
# CRYPTO TRADE PIPELINE - COMPLETE
# ============================================

import csv, random
from datetime import datetime, timedelta
import subprocess
import time

print("=" * 60)
print("🚀 CRYPTO TRADE PIPELINE - SETUP")
print("=" * 60)

# ============================================
# STEP 0: Create Sample Data
# ============================================

print("\n📊 Generating sample crypto trade data...")

random.seed(42)
exchanges = ["BINANCE", "COINBASE", "KRAKEN", "BYBIT", "OKX"]
coins = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
rows = []
start = datetime(2026, 8, 1, 8, 0, 0)

for i in range(2000):
    trade_id = f"TRD{i:06d}"
    exchange = random.choice(exchanges)
    coin = random.choice(coins)
    trader_id = f"TRADER{random.randint(1, 300):04d}"
    amount_usd = round(random.choice([
        random.uniform(50, 5000),
        random.uniform(100000, 500000)
    ]), 2)
    ts = start + timedelta(seconds=random.randint(0, 6 * 3600))
    
    if i % 137 == 0:
        amount_usd = None
    if i % 211 == 0:
        exchange = None
    
    rows.append([trade_id, exchange, coin, trader_id, amount_usd, ts.isoformat()])

with open("crypto_sample.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["trade_id", "exchange", "coin", "trader_id", "amount_usd", "event_time"])
    w.writerows(rows)

print(f"✅ Wrote {len(rows)} rows to crypto_sample.csv")

# ============================================
# STEP 1: Upload to HDFS
# ============================================

print("\n📤 Uploading to HDFS...")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/training/crypto"], check=False)
subprocess.run(["hdfs", "dfs", "-put", "-f", "crypto_sample.csv", "/training/crypto/crypto_sample.csv"], check=False)
print("✅ File uploaded to HDFS")

# ============================================
# STEP 2: PySpark Imports
# ============================================

print("\n" + "=" * 60)
print("🔥 PYSPARK CRYPTO TRADE PIPELINE")
print("=" * 60)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================
# EXERCISE 1: SparkSession and Data Loading
# ============================================

print("\n📊 EXERCISE 1: SparkSession and Data Loading")
print("-" * 40)

spark = SparkSession.builder \
    .appName("Crypto-Practice") \
    .master("local[*]") \
    .getOrCreate()

print(f"✅ Spark session created: {spark.version}")

# Read from HDFS or local
try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("/training/crypto/crypto_sample.csv")
except:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("crypto_sample.csv")

print("\n📋 Schema:")
df.printSchema()

print("\n🔍 First 10 rows:")
df.show(10, truncate=False)

print(f"\n📊 Total rows: {df.count()}")

# ============================================
# EXERCISE 2: Filtering and Selecting
# ============================================

print("\n" + "=" * 60)
print("📊 EXERCISE 2: Filtering and Selecting")
print("=" * 60)

whale_trades = df.filter(col("amount_usd") > 100000)
selected_trades = whale_trades.select("trade_id", "exchange", "amount_usd")

print("\n🔍 Whale trades ($100,000+):")
selected_trades.show(10)

print(f"\n📊 Total whale trades: {selected_trades.count()}")

# ============================================
# EXERCISE 3: Cleaning Dirty Data
# ============================================

print("\n" + "=" * 60)
print("🧹 EXERCISE 3: Cleaning Dirty Data")
print("=" * 60)

null_amount = df.filter(col("amount_usd").isNull()).count()
null_exchange = df.filter(col("exchange").isNull()).count()

print(f"📊 Rows with null amount_usd: {null_amount}")
print(f"📊 Rows with null exchange: {null_exchange}")

clean_df = df.dropna(subset=["amount_usd", "exchange"])
clean_df = clean_df.withColumn("is_whale_trade", col("amount_usd") > 100000)

print(f"\n📊 Original rows: {df.count()}")
print(f"📊 Cleaned rows: {clean_df.count()}")
print(f"📊 Rows removed: {df.count() - clean_df.count()}")

print("\n🔍 Cleaned data sample:")
clean_df.show(5)

# ============================================
# EXERCISE 4: GroupBy and Aggregation
# ============================================

print("\n" + "=" * 60)
print("📊 EXERCISE 4: GroupBy and Aggregation")
print("=" * 60)

exchange_stats = clean_df.groupBy("exchange").agg(
    count("*").alias("trade_count"),
    sum("amount_usd").alias("total_volume"),
    avg("amount_usd").alias("avg_trade_size"),
    max("amount_usd").alias("max_trade_size")
).orderBy(col("total_volume").desc())

print("\n📊 Exchange Statistics:")
exchange_stats.show()

# ============================================
# EXERCISE 5: Joining DataFrames
# ============================================

print("\n" + "=" * 60)
print("🔗 EXERCISE 5: Joining DataFrames")
print("=" * 60)

trader_data = [
    ("TRADER0001", "Alice Johnson", "LOW"),
    ("TRADER0005", "Bob Smith", "MEDIUM"),
    ("TRADER0010", "Charlie Brown", "HIGH"),
    ("TRADER0020", "Diana Prince", "LOW"),
    ("TRADER0050", "Eve Wilson", "HIGH")
]
trader_df = spark.createDataFrame(trader_data, ["trader_id", "trader_name", "risk_level"])

print("📋 Trader Lookup Table:")
trader_df.show()

joined_df = clean_df.join(trader_df, on="trader_id", how="left")
unmatched = joined_df.filter(col("trader_name").isNull())

print(f"\n📊 Unmatched trades: {unmatched.count()}")

if unmatched.count() > 0:
    print("\n🔍 Sample unmatched trades:")
    unmatched.select("trade_id", "trader_id", "amount_usd").show(5)

# ============================================
# EXERCISE 6: Caching
# ============================================

print("\n" + "=" * 60)
print("💾 EXERCISE 6: Caching")
print("=" * 60)

print("\n🔄 Without caching:")
start = time.time()
joined_df.count()
print(f"   First count: {time.time() - start:.4f}s")

start = time.time()
joined_df.count()
print(f"   Second count: {time.time() - start:.4f}s")

print("\n💾 With caching:")
joined_df.cache()
start = time.time()
joined_df.count()
print(f"   First count (cached): {time.time() - start:.4f}s")

start = time.time()
joined_df.count()
print(f"   Second count (cached): {time.time() - start:.4f}s")

joined_df.unpersist()

# ============================================
# EXERCISE 7: Writing Results to HDFS
# ============================================

print("\n" + "=" * 60)
print("💾 EXERCISE 7: Writing Results to HDFS")
print("=" * 60)

output_path = "/training/crypto/output/by_exchange"
try:
    exchange_stats.write.mode("overwrite").parquet(output_path)
    print(f"✅ Results written to: {output_path}")
    
    verify_df = spark.read.parquet(output_path)
    print(f"📊 Verification - rows: {verify_df.count()}")
    verify_df.show()
except:
    print("⚠️ Writing to local instead...")
    exchange_stats.write.mode("overwrite").parquet("local_output")
    print("✅ Results saved locally")

# ============================================
# SUMMARY
# ============================================

print("\n" + "=" * 60)
print("✅ CRYPTO TRADE PIPELINE COMPLETE!")
print("=" * 60)

print(f"\n📊 Summary:")
print(f"   - Total trades: {df.count()}")
print(f"   - Cleaned trades: {clean_df.count()}")
print(f"   - Whale trades: {selected_trades.count()}")
print(f"   - Top exchange: {exchange_stats.first()['exchange']}")

spark.stop()
print("\n🔥 Spark session stopped.")
