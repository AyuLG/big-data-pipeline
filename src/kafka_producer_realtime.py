#!/usr/bin/env python3
import json
import time
import pandas as pd
import random
import argparse
from kafka import KafkaProducer
from datetime import datetime, timedelta

def stream_realtime_data(csv_path, topic, bootstrap_servers, rate=2.0):
    print(f"📂 Loading patterns from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"📊 Loaded {len(df)} patterns")
    
    # Pre-compute statistics for realistic generation
    bank_distribution = df['From Bank'].value_counts().head(1000).index.tolist()
    currency_distribution = df['Receiving Currency'].value_counts().index.tolist()
    format_distribution = df['Payment Format'].value_counts().index.tolist()
    laundering_rate = df['Is Laundering'].mean()
    
    print(f"📊 Laundering rate: {laundering_rate*100:.2f}%")
    print(f"🏦 Using {len(bank_distribution)} active banks")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=5
    )
    
    print(f"\n🚀 Streaming real-time transactions to: {topic}")
    print(f"⏱️ Rate: {rate} messages/second")
    print("🔄 Press Ctrl+C to stop.\n")
    
    sent = 0
    
    try:
        while True:
            from_bank = random.choice(bank_distribution)
            to_bank = random.choice(bank_distribution)
            amount = round(random.uniform(5, 50000), 2)
            is_laundering = 1 if random.random() < laundering_rate else 0
            
            record = {
                "Timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 60))).strftime("%Y/%m/%d %H:%M"),
                "From Bank": from_bank,
                "Account": f"{random.randint(1000000, 9999999):07X}",
                "To Bank": to_bank,
                "Account.1": f"{random.randint(1000000, 9999999):07X}",
                "Amount Received": amount,
                "Receiving Currency": random.choice(currency_distribution),
                "Amount Paid": amount,
                "Payment Currency": random.choice(currency_distribution),
                "Payment Format": random.choice(format_distribution),
                "Is Laundering": is_laundering,
                "kafka_timestamp": datetime.now().isoformat()
            }
            
            key = str(from_bank)
            
            future = producer.send(topic, key=key, value=record)
            future.get(timeout=10)
            
            sent += 1
            
            if sent % 10 == 0:
                flag = "⚠️ LAUNDERING!" if is_laundering else ""
                print(f"[{sent:6d}] | {record['Timestamp']} | "
                      f"{amount:>12.2f} | Bank {from_bank} → {to_bank} {flag}")
            
            time.sleep(1.0 / rate)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Stopped by user")
    finally:
        producer.flush()
        producer.close()
        print("\n" + "="*60)
        print(f"✅ Total messages sent: {sent}")
        print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ibm_aml_data.csv")
    parser.add_argument("--topic", default="transactions_ibm")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=2.0)
    args = parser.parse_args()
    
    stream_realtime_data(args.csv, args.topic, args.bootstrap_servers, args.rate)
