#!/usr/bin/env python3
# ============================================
# CRYPTO PRODUCER FOR STREAMING
# Sends JSON messages to Kafka topic "crypto-practice"
# ============================================

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

print("=" * 60)
print("📤 CRYPTO PRODUCER - Streaming to Kafka")
print("=" * 60)

# Configuration
exchanges = ["BINANCE", "COINBASE", "KRAKEN", "BYBIT", "OKX"]
coins = ["BTC", "ETH", "SOL", "XRP", "DOGE"]

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks=1
)

print("✅ Kafka producer created")

# Generate and send messages
sent = 0
try:
    print("\n🚀 Sending messages to crypto-practice")
    print("Press Ctrl+C to stop\n")
    print("-" * 60)
    
    while True:
        # Generate random trade
        trade = {
            "trade_id": f"STRD{sent:06d}",
            "exchange": random.choice(exchanges),
            "coin": random.choice(coins),
            "trader_id": f"TRADER{random.randint(1, 100):04d}",
            "amount_usd": round(random.uniform(50, 50000), 2),
            "event_time": datetime.now().isoformat()
        }
        
        # Send to Kafka
        future = producer.send("crypto-practice", value=trade)
        future.get(timeout=5)
        
        sent += 1
        
        # Print every 5 messages
        if sent % 5 == 0:
            print(f"[{sent:4d}] {trade['trade_id']} | {trade['exchange']} | "
                  f"${trade['amount_usd']:>10.2f} | {trade['coin']}")
        
        time.sleep(0.5)  # 2 messages per second
        
except KeyboardInterrupt:
    print(f"\n⏹️ Stopped by user")
finally:
    producer.flush()
    producer.close()
    print(f"\n✅ Total messages sent: {sent}")
    print("=" * 60)
