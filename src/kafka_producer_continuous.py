#!/usr/bin/env python3
import json
import time
import pandas as pd
import argparse
from kafka import KafkaProducer
from datetime import datetime

def stream_continuous_data(csv_path, topic, bootstrap_servers, rate=2.0):
    print(f"📂 Reading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"📊 Loaded {len(df)} transactions for continuous streaming")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=5
    )
    
    print(f"\n🚀 Continuous streaming to Kafka topic: {topic}")
    print(f"⏱️ Rate: {rate} messages/second")
    print("🔄 Will loop through all records continuously. Press Ctrl+C to stop.")
    print("-"*60)
    
    sent = 0
    total_sent = 0
    cycle = 0
    
    try:
        while True:
            cycle += 1
            print(f"\n📦 Cycle {cycle} - Streaming {len(df)} records...")
            
            for idx, row in df.iterrows():
                record = row.to_dict()
                key = str(record.get('From Bank', 'unknown'))
                record['kafka_timestamp'] = datetime.now().isoformat()
                
                future = producer.send(topic, key=key, value=record)
                future.get(timeout=10)
                
                sent += 1
                total_sent += 1
                
                if sent % 100 == 0 or sent <= 10:
                    laundering = "⚠️ LAUNDERING!" if record.get('Is Laundering', 0) == 1 else ""
                    amount = record.get('Amount Received', 0)
                    print(f"[{sent:4d}/{len(df)}] | Total: {total_sent:6d} | "
                          f"{amount:>12.2f} | Bank {record.get('From Bank', 'N/A')} {laundering}")
                
                time.sleep(1.0 / rate)
            
            sent = 0
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Stopped by user after {total_sent} total messages sent")
    finally:
        producer.flush()
        producer.close()
        print("\n" + "="*60)
        print(f"✅ Total messages sent: {total_sent}")
        print(f"📤 Topic: {topic}")
        print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ibm_aml_data.csv")
    parser.add_argument("--topic", default="transactions_ibm")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=2.0)
    args = parser.parse_args()
    
    stream_continuous_data(args.csv, args.topic, args.bootstrap_servers, args.rate)
