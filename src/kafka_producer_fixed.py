#!/usr/bin/env python3
import json
import time
import pandas as pd
import argparse
from kafka import KafkaProducer
from datetime import datetime

def stream_data_to_kafka(csv_path, topic, bootstrap_servers, rate=2.0, limit=None):
    print(f"📂 Reading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    
    if limit:
        df = df.head(limit)
    
    print(f"📊 Loaded {len(df)} transactions")
    print(f"📋 Columns: {df.columns.tolist()}")
    
    df = df.fillna('')
    
    # Create Kafka producer with shorter timeout
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks=1,  # Changed from 'all' to 1 for faster response
        retries=3,
        request_timeout_ms=5000,  # 5 seconds timeout
        max_block_ms=5000,  # 5 seconds max block
    )
    
    print(f"\n🚀 Streaming to Kafka topic: {topic}")
    print(f"⏱️ Rate: {rate} messages/second")
    print("-"*60)
    
    sent = 0
    try:
        for idx, row in df.iterrows():
            record = row.to_dict()
            key = str(record.get('From Bank', 'unknown'))
            record['kafka_timestamp'] = datetime.now().isoformat()
            
            # Send to Kafka with timeout
            future = producer.send(topic, key=key, value=record)
            future.get(timeout=5)  # 5 second timeout
            
            sent += 1
            laundering = "⚠️ LAUNDERING!" if record.get('Is Laundering', 0) == 1 else ""
            amount = record.get('Amount Received', 0)
            
            print(f"[{sent:4d}/{len(df)}] {record.get('Timestamp', 'N/A')[:16]} | "
                  f"{amount:>12.2f} | Bank {record.get('From Bank', 'N/A')} {laundering}")
            
            time.sleep(1.0 / rate)
            
    except KeyboardInterrupt:
        print("\n⏹️ Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        producer.flush()
        producer.close()
        print("\n" + "="*60)
        print(f"✅ Total messages sent: {sent}")
        print(f"📤 Topic: {topic}")
        print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="ibm_aml_data.csv")
    parser.add_argument("--topic", default="transactions_ibm")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    
    stream_data_to_kafka(args.csv, args.topic, args.bootstrap_servers, args.rate, args.limit)
