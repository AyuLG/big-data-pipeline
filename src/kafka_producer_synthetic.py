#!/usr/bin/env python3
import json
import time
import random
import argparse
from kafka import KafkaProducer
from datetime import datetime

BANKS = list(range(1, 50000))
CURRENCIES = ['US Dollar', 'Euro', 'Yuan', 'Ruble', 'Yen', 'UK Pound']
PAYMENT_FORMATS = ['Reinvestment', 'Cheque', 'Credit Card', 'ACH', 'Wire']

def generate_transaction():
    from_bank = random.choice(BANKS)
    to_bank = random.choice(BANKS)
    amount = round(random.uniform(1, 50000), 2)
    is_laundering = 1 if random.random() < 0.001 else 0
    
    return {
        "Timestamp": datetime.now().strftime("%Y/%m/%d %H:%M"),
        "From Bank": from_bank,
        "Account": f"{random.randint(1000000, 9999999):07X}",
        "To Bank": to_bank,
        "Account.1": f"{random.randint(1000000, 9999999):07X}",
        "Amount Received": amount,
        "Receiving Currency": random.choice(CURRENCIES),
        "Amount Paid": amount,
        "Payment Currency": random.choice(CURRENCIES),
        "Payment Format": random.choice(PAYMENT_FORMATS),
        "Is Laundering": is_laundering,
        "kafka_timestamp": datetime.now().isoformat()
    }

def stream_synthetic_data(topic, bootstrap_servers, rate=2.0):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=5
    )
    
    print(f"🚀 Streaming synthetic transactions to: {topic}")
    print(f"⏱️ Rate: {rate} messages/second")
    print("🔄 Each transaction is unique. Press Ctrl+C to stop.")
    print("-"*60)
    
    sent = 0
    laundering_count = 0
    
    try:
        while True:
            record = generate_transaction()
            key = str(record.get('From Bank', 'unknown'))
            
            future = producer.send(topic, key=key, value=record)
            future.get(timeout=10)
            
            sent += 1
            if record['Is Laundering'] == 1:
                laundering_count += 1
            
            if sent % 10 == 0:
                print(f"[{sent:6d}] | Amount: {record['Amount Received']:>12.2f} | "
                      f"From: {record['From Bank']} → To: {record['To Bank']} | "
                      f"Laundering: {'⚠️ YES' if record['Is Laundering'] else 'No'}")
            
            time.sleep(1.0 / rate)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Stopped by user")
    finally:
        producer.flush()
        producer.close()
        print("\n" + "="*60)
        print(f"✅ Total messages sent: {sent}")
        print(f"🚨 Laundering cases: {laundering_count} ({laundering_count/sent*100:.2f}%)")
        print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="transactions_ibm")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=2.0)
    args = parser.parse_args()
    
    stream_synthetic_data(args.topic, args.bootstrap_servers, args.rate)
