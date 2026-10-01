"""
Producer 3 of 3 (LIVE): WMS & Scanner Producer

Runs forever, and does TWO jobs at once, matching how a real warehouse
scanning system behaves:

  1. REACTS to the other two producers -- it consumes
     purchase-orders-topic and sales-orders-topic, and the moment it
     sees a GOODS_RECEIVED or STOCK_ISSUED status, it publishes the
     matching physical stock movement. This is what keeps stock
     movements correctly cross-referenced to real PO/order IDs even
     though this is a completely separate process with no shared memory.

  2. INDEPENDENTLY generates its own events that don't depend on the
     other two systems: warehouse transfers (with two linked movements
     when received), cycle counts, and adjustments (returns, damage,
     batch assignment).

Publishes to: stock-movement-topic, transfers-topic, adjustments-topic

Usage:
    python wms_scanner_producer_live.py
    Press Ctrl+C to stop.
"""

import json
import random
import sys
import time
import uuid
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer, errors

KAFKA_BROKER = 'localhost:9092'

WAREHOUSES = ["WH-DEL-01", "WH-DEL-02", "WH-BLR-01", "WH-BLR-02", "WH-MUM-01"]
CATEGORIES = ["Apparel", "Electronics", "Home & Kitchen", "Sports & Outdoors", "Beauty & Personal Care"]
SKUS = [(f"PRD-{6000+i}", f"SKU-{6000+i}-{random.choice(['BLK','WHT','RED','BLU'])}-{random.choice(['S','M','L'])}",
         random.choice(CATEGORIES)) for i in range(35)]

ADJ_TYPES = ["RETURN_TO_STOCK", "DAMAGE_WRITE_OFF", "BATCH_ASSIGNED"]
REASONS = {
    "RETURN_TO_STOCK": ["Customer returned unused item with tags attached", "Customer refused delivery unopened", "Wrong size ordered"],
    "DAMAGE_WRITE_OFF": ["Water damage found during putaway inspection", "Item damaged in transit", "Packaging crushed on receiving dock"],
    "BATCH_ASSIGNED": ["Batch number assigned to inbound receipt", "Batch relabelled after QC hold"],
}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_and_count(producer, topic_counts, topic, key, value, label=""):
    producer.send(topic, key=key, value=value)
    topic_counts[topic] = topic_counts.get(topic, 0) + 1
    suffix = f" ({label})" if label else ""
    print(f"[{now_str()}] -> {topic}{suffix}: {value}")


def bin_location():
    return f"{random.choice('ABC')}-{random.randint(1,20):02d}-{random.randint(1,15):02d}"


def batch_no():
    return f"BT-{datetime.now().strftime('%Y%m')}-{random.randint(10,99)}"


def emit_transfer_and_maybe_movements(producer, topic_counts, open_transfers, transfer_counter):
    if open_transfers and random.random() < 0.7:
        transfer_id = random.choice(list(open_transfers.keys()))
        tr = open_transfers[transfer_id]
    else:
        transfer_id = f"TR-{transfer_counter[0]}"
        transfer_counter[0] += 1
        product_id, sku, category = random.choice(SKUS)
        src, dst = random.sample(WAREHOUSES, 2)
        tr = {
            "product_id": product_id, "sku": sku, "category": category,
            "quantity": random.choice([5, 10, 15, 20]),
            "source_warehouse_id": src, "destination_warehouse_id": dst,
            "stage": 0,
        }
        open_transfers[transfer_id] = tr

    stages = ["TRANSFER_RAISED", "IN_TRANSIT", "RECEIVED"]
    stage = stages[tr["stage"]]
    payload = {
        "timestamp": now_str(), "transfer_id": transfer_id, "product_id": tr["product_id"], "sku": tr["sku"],
        "quantity": tr["quantity"], "source_warehouse_id": tr["source_warehouse_id"],
        "destination_warehouse_id": tr["destination_warehouse_id"], "status": stage,
    }
    send_and_count(producer, topic_counts, "transfers-topic", transfer_id, payload)

    if stage == "RECEIVED":
        bno = batch_no()
        out_move = {
            "movement_id": f"MOV-{uuid.uuid4().hex[:8]}", "event_type": "STOCK_ISSUE", "event_timestamp": now_str(),
            "product_id": tr["product_id"], "sku": tr["sku"], "category": tr["category"],
            "warehouse_id": tr["source_warehouse_id"], "bin_location": bin_location(),
            "quantity": -tr["quantity"], "unit_cost": round(random.uniform(150, 1800), 2),
            "reference_type": "TRANSFER", "reference_id": transfer_id, "batch_no": bno, "serial_no": "",
        }
        in_move = dict(out_move, movement_id=f"MOV-{uuid.uuid4().hex[:8]}", event_type="STOCK_RECEIPT",
                       warehouse_id=tr["destination_warehouse_id"], bin_location=bin_location(), quantity=tr["quantity"])
        for mv in (out_move, in_move):
            send_and_count(producer, topic_counts, "stock-movement-topic", f"{mv['sku']}:{mv['warehouse_id']}", mv)

    tr["stage"] += 1
    if tr["stage"] >= len(stages):
        del open_transfers[transfer_id]


def emit_cycle_count(producer, topic_counts, cc_counter):
    product_id, sku, category = random.choice(SKUS)
    warehouse_id = random.choice(WAREHOUSES)
    payload = {
        "movement_id": f"MOV-{uuid.uuid4().hex[:8]}", "event_type": "CYCLE_COUNT", "event_timestamp": now_str(),
        "product_id": product_id, "sku": sku, "category": category, "warehouse_id": warehouse_id,
        "bin_location": bin_location(), "quantity": random.choice([-3, -2, -1, 1, 2]),
        "unit_cost": round(random.uniform(150, 1800), 2), "reference_type": "CYCLE_COUNT",
        "reference_id": f"CC-{cc_counter[0]}", "batch_no": "", "serial_no": "",
    }
    cc_counter[0] += 1
    send_and_count(producer, topic_counts, "stock-movement-topic", f"{sku}:{warehouse_id}", payload)


def emit_adjustment(producer, topic_counts, adj_counter):
    product_id, sku, category = random.choice(SKUS)
    warehouse_id = random.choice(WAREHOUSES)
    adj_type = random.choice(ADJ_TYPES)
    qty = {"RETURN_TO_STOCK": random.randint(1, 3), "DAMAGE_WRITE_OFF": -random.randint(1, 4), "BATCH_ASSIGNED": 0}[adj_type]
    payload = {
        "timestamp": now_str(), "adjustment_id": f"ADJ-{adj_counter[0]}", "product_id": product_id, "sku": sku,
        "warehouse_id": warehouse_id, "adjustment_type": adj_type, "quantity": qty,
        "reason": random.choice(REASONS[adj_type]), "batch_no": batch_no(), "serial_no": "",
    }
    adj_counter[0] += 1
    send_and_count(producer, topic_counts, "adjustments-topic", f"{sku}:{warehouse_id}", payload)


def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            retries=5
        )
        consumer = KafkaConsumer(
            'purchase-orders-topic', 'sales-orders-topic',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',   # only react to NEW events from here on, not history
            enable_auto_commit=True,
            group_id='wms-scanner-reaction-group',
            consumer_timeout_ms=200,       # poll returns quickly so we can also generate our own events
        )
        print("✅ Connected to Kafka (producing + reacting to purchase-orders-topic / sales-orders-topic)")
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available")
        sys.exit(1)

    open_transfers = {}
    transfer_counter = [2000]
    cc_counter = [4000]
    adj_counter = [9000]
    topic_counts = {}

    print("📡 WMS & Scanner Producer streaming continuously. Ctrl+C to stop.\n")
    try:
        while True:
            # 1. react to any new PO / sales-order events since last poll
            for message in consumer:
                topic = message.topic
                row = message.value
                sku = row.get('sku')
                warehouse_id = row.get('warehouse_id')
                if not sku or not warehouse_id:
                    continue

                if topic == 'purchase-orders-topic' and row.get('status') == 'GOODS_RECEIVED':
                    payload = {
                        "movement_id": f"MOV-{uuid.uuid4().hex[:8]}", "event_type": "STOCK_RECEIPT",
                        "event_timestamp": now_str(), "product_id": row.get('product_id'), "sku": sku,
                        "category": random.choice(CATEGORIES), "warehouse_id": warehouse_id,
                        "bin_location": bin_location(), "quantity": row.get('quantity'),
                        "unit_cost": row.get('unit_cost'), "reference_type": "PURCHASE_ORDER",
                        "reference_id": row.get('po_id'), "batch_no": batch_no(), "serial_no": "",
                    }
                    send_and_count(producer, topic_counts, "stock-movement-topic",
                                   f"{sku}:{warehouse_id}", payload, label="reacting to PO receipt")

                elif topic == 'sales-orders-topic' and row.get('status') == 'STOCK_ISSUED':
                    payload = {
                        "movement_id": f"MOV-{uuid.uuid4().hex[:8]}", "event_type": "STOCK_ISSUE",
                        "event_timestamp": now_str(), "product_id": row.get('product_id'), "sku": sku,
                        "category": random.choice(CATEGORIES), "warehouse_id": warehouse_id,
                        "bin_location": bin_location(), "quantity": -int(row.get('quantity', 0) or 0),
                        "unit_cost": round(float(row.get('unit_price', 0) or 0) * 0.6, 2),
                        "reference_type": "SALES_ORDER", "reference_id": row.get('order_id'),
                        "batch_no": batch_no(), "serial_no": "",
                    }
                    send_and_count(producer, topic_counts, "stock-movement-topic",
                                   f"{sku}:{warehouse_id}", payload, label="reacting to order issue")

            # 2. independently generate WMS-only events
            roll = random.random()
            if roll < 0.5:
                emit_cycle_count(producer, topic_counts, cc_counter)
            elif roll < 0.8:
                emit_transfer_and_maybe_movements(producer, topic_counts, open_transfers, transfer_counter)
            else:
                emit_adjustment(producer, topic_counts, adj_counter)

            time.sleep(max(0.05, random.expovariate(1.0 / 1.0)))
    except KeyboardInterrupt:
        producer.flush()
        total = sum(topic_counts.values())
        print(f"\n🛑 Stopped. Published {total} events total.")
        print("Events sent per topic:")
        for topic, n in sorted(topic_counts.items()):
            print(f"  {topic}: {n}")


if __name__ == '__main__':
    main()
