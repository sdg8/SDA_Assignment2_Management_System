"""
Producer 1 of 3 (LIVE): Procurement Producer

Runs forever. Generates new purchase orders and progresses open ones
through their lifecycle (PO_RAISED -> PO_APPROVED -> GOODS_RECEIVED ->
PO_CLOSED), publishing each status change to purchase-orders-topic as
it happens in real time.

This process knows nothing about stock-movement-topic or the WMS --
in a real system these are separate services. The WMS & Scanner
Producer listens to this topic and reacts to GOODS_RECEIVED on its own.

Usage:
    python procurement_producer_live.py
    python procurement_producer_live.py --rate 1.5   # avg seconds between events
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
TOPIC = 'purchase-orders-topic'

WAREHOUSES = ["WH-DEL-01", "WH-DEL-02", "WH-BLR-01", "WH-BLR-02", "WH-MUM-01"]
SKUS = [(f"PRD-{6000+i}", f"SKU-{6000+i}-{random.choice(['BLK','WHT','RED','BLU'])}-{random.choice(['S','M','L'])}")
        for i in range(35)]
SUPPLIERS = [f"SUP-{random.randint(10, 99)}" for _ in range(10)]
PO_STAGES = ["PO_RAISED", "PO_APPROVED", "GOODS_RECEIVED", "PO_CLOSED"]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=1.5, help="average seconds between events")
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

    open_pos = {}
    po_counter = 1000
    topic_counts = {}

    print(f"📡 Procurement Producer streaming continuously (avg {args.rate}s/event). Ctrl+C to stop.\n")
    try:
        while True:
            if open_pos and random.random() < 0.7:
                po_id = random.choice(list(open_pos.keys()))
                po = open_pos[po_id]
            else:
                po_id = f"PO-{po_counter}"
                po_counter += 1
                product_id, sku = random.choice(SKUS)
                po = {
                    "product_id": product_id, "sku": sku,
                    "warehouse_id": random.choice(WAREHOUSES),
                    "quantity": random.choice([20, 30, 40, 50, 75, 100]),
                    "unit_cost": round(random.uniform(150, 1800), 2),
                    "stage": 0,
                }
                open_pos[po_id] = po

            stage = PO_STAGES[po["stage"]]
            payload = {
                "timestamp": now_str(), "po_id": po_id, "supplier_id": random.choice(SUPPLIERS),
                "product_id": po["product_id"], "sku": po["sku"], "warehouse_id": po["warehouse_id"],
                "quantity": po["quantity"], "unit_cost": po["unit_cost"], "status": stage,
            }
            producer.send(TOPIC, key=po_id, value=payload)
            print(f"[{now_str()}] -> {TOPIC}: {payload}")
            topic_counts[TOPIC] = topic_counts.get(TOPIC, 0) + 1

            po["stage"] += 1
            if po["stage"] >= len(PO_STAGES):
                del open_pos[po_id]

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
