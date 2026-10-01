"""
Producer 2 of 3 (LIVE): Sales / POS Producer

Runs forever. Generates new sales orders and progresses open ones
through their lifecycle (ORDER_PLACED -> STOCK_RESERVED -> STOCK_ISSUED,
or CANCELLED ~12% of the time), publishing each status change to
sales-orders-topic as it happens in real time.

This process knows nothing about stock-movement-topic -- the WMS &
Scanner Producer listens to this topic and reacts to STOCK_ISSUED on
its own.

Usage:
    python sales_pos_producer_live.py
    python sales_pos_producer_live.py --rate 0.5   # avg seconds between events (busier store)
    Press Ctrl+C to stop.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer, errors

fake = Faker()
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sales-orders-topic'

WAREHOUSES = ["WH-DEL-01", "WH-DEL-02", "WH-BLR-01", "WH-BLR-02", "WH-MUM-01"]
SKUS = [(f"PRD-{6000+i}", f"SKU-{6000+i}-{random.choice(['BLK','WHT','RED','BLU'])}-{random.choice(['S','M','L'])}")
        for i in range(35)]
ORDER_STAGES = ["ORDER_PLACED", "STOCK_RESERVED", "STOCK_ISSUED"]
ORDER_STAGES_CANCELLED = ["ORDER_PLACED", "STOCK_RESERVED", "CANCELLED"]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=0.8, help="average seconds between events")
    args = parser.parse_args()

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            retries=5
        )
        print(f"✅ Connected to Kafka, publishing to '{TOPIC}'")
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available")
        sys.exit(1)

    open_orders = {}
    topic_counts = {}

    print(f"📡 Sales/POS Producer streaming continuously (avg {args.rate}s/event). Ctrl+C to stop.\n")
    try:
        while True:
            if open_orders and random.random() < 0.7:
                order_id = random.choice(list(open_orders.keys()))
                order = open_orders[order_id]
            else:
                order_id = f"ORD-{fake.random_number(digits=8, fix_len=True)}"
                product_id, sku = random.choice(SKUS)
                order = {
                    "customer_id": f"CUST-{fake.random_number(digits=5, fix_len=True)}",
                    "product_id": product_id, "sku": sku,
                    "warehouse_id": random.choice(WAREHOUSES),
                    "quantity": random.randint(1, 4),
                    "unit_price": round(random.uniform(399, 4999), 2),
                    "stage": 0,
                    "cancelled": random.random() < 0.12,
                }
                open_orders[order_id] = order

            stages = ORDER_STAGES_CANCELLED if order["cancelled"] else ORDER_STAGES
            stage = stages[order["stage"]]
            payload = {
                "timestamp": now_str(), "order_id": order_id, "customer_id": order["customer_id"],
                "product_id": order["product_id"], "sku": order["sku"], "warehouse_id": order["warehouse_id"],
                "quantity": order["quantity"], "unit_price": order["unit_price"], "status": stage,
            }
            producer.send(TOPIC, key=order_id, value=payload)
            print(f"[{now_str()}] -> {TOPIC}: {payload}")
            topic_counts[TOPIC] = topic_counts.get(TOPIC, 0) + 1

            order["stage"] += 1
            if order["stage"] >= len(stages):
                del open_orders[order_id]

            time.sleep(max(0.05, random.expovariate(1.0 / args.rate)))
    except KeyboardInterrupt:
        producer.flush()
        total = sum(topic_counts.values())
        print(f"\n🛑 Stopped. Published {total} events total.")
        print("Events sent per topic:")
        for topic, n in sorted(topic_counts.items()):
            print(f"  {topic}: {n}")


if __name__ == '__main__':
    main()
