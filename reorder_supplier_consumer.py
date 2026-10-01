"""
Consumer Group B: Reorder & Supplier Consumer

Subscribes to THREE topics, matching the architecture diagram:
    - purchase-orders-topic  (to measure supplier lead time)
    - stock-movement-topic   (to track its own view of on-hand stock)
    - transfers-topic        (to track stock currently in transit)

This is a SEPARATE consumer group from Group A, so Kafka gives it its
own independent copy of purchase-orders-topic and stock-movement-topic
-- it is not competing with Group A for messages. It deliberately keeps
its own local view of stock rather than reading Group A's MySQL table,
because that is the point of decoupled consumer groups: this consumer
would keep working even if Group A's process were down.

Two things this consumer produces that Group A does not:
    1. reorder_alerts        -- fires when on_hand + in_transit falls
                                 below the reorder threshold
    2. supplier_performance   -- lead time (PO_RAISED -> GOODS_RECEIVED)
                                 per purchase order, for supplier scorecards

Writes to BOTH stores:
    MySQL   -> reorder_watch, reorder_alerts, supplier_performance
               (current state + structured alert/performance records)
    MongoDB -> reorder_events: a raw, append-only log of every event
               this consumer processed, for audit/history purposes.

Usage:
    python reorder_supplier_consumer.py
"""

import json
import sys
from datetime import datetime

from kafka import KafkaConsumer, errors
import mysql.connector
from pymongo import MongoClient

KAFKA_BROKER = 'localhost:9092'
TOPICS = ['purchase-orders-topic', 'stock-movement-topic', 'transfers-topic']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': 'root',
    'database': 'stockdb'
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = 'stockdb'

REORDER_THRESHOLD = 20
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def ensure_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reorder_watch (
            sku VARCHAR(30),
            warehouse_id VARCHAR(20),
            on_hand_estimate INT DEFAULT 0,
            in_transit INT DEFAULT 0,
            last_updated VARCHAR(50),
            PRIMARY KEY (sku, warehouse_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reorder_alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sku VARCHAR(30),
            warehouse_id VARCHAR(20),
            available_estimate INT,
            triggered_at VARCHAR(50)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS supplier_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            po_id VARCHAR(20),
            supplier_id VARCHAR(20),
            sku VARCHAR(30),
            quantity INT,
            unit_cost DECIMAL(10,2),
            po_raised_at VARCHAR(50),
            goods_received_at VARCHAR(50),
            lead_time_hours DECIMAL(8,2)
        )
    """)


def get_watch_row(cursor, sku, warehouse_id):
    cursor.execute(
        "SELECT on_hand_estimate, in_transit FROM reorder_watch WHERE sku=%s AND warehouse_id=%s",
        (sku, warehouse_id)
    )
    row = cursor.fetchone()
    return row if row else (0, 0)


def upsert_watch(cursor, sku, warehouse_id, on_hand, in_transit, timestamp):
    cursor.execute("""
        INSERT INTO reorder_watch (sku, warehouse_id, on_hand_estimate, in_transit, last_updated)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            on_hand_estimate = VALUES(on_hand_estimate),
            in_transit = VALUES(in_transit),
            last_updated = VALUES(last_updated)
    """, (sku, warehouse_id, on_hand, in_transit, timestamp))


def main():
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='reorder-supplier-group'
        )
        print(f"✅ Connected to Kafka, subscribed to {TOPICS}")
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available")
        sys.exit(1)

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        ensure_tables(cursor)
        conn.commit()
        print("✅ Connected to MySQL, ensured reorder/supplier tables exist")
    except Exception as e:
        print("❌ Error connecting to MySQL:", e)
        sys.exit(1)

    try:
        mongo_client = MongoClient(MONGO_URI)
        events_collection = mongo_client[MONGO_DB]['reorder_events']
        print(f"✅ Connected → {mongo_client[MONGO_DB].name}.{events_collection.name}")
    except Exception as e:
        print("❌ Error connecting to MongoDB:", e)
        sys.exit(1)

    # in-memory tracker of when each PO was raised, to compute lead time later
    po_raised_at = {}

    print(f"📥 [Group B] Listening on {TOPICS} ...")
    saved = 0
    topic_counts = {}
    try:
        for message in consumer:
            topic = message.topic
            row = message.value
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

            if topic == 'purchase-orders-topic':
                po_id = row.get('po_id')
                status = row.get('status')
                if status == 'PO_RAISED':
                    po_raised_at[po_id] = row.get('timestamp')
                elif status == 'GOODS_RECEIVED' and po_id in po_raised_at:
                    try:
                        t0 = datetime.strptime(po_raised_at[po_id], TS_FORMAT)
                        t1 = datetime.strptime(row.get('timestamp'), TS_FORMAT)
                        lead_hours = round((t1 - t0).total_seconds() / 3600, 2)
                        cursor.execute("""
                            INSERT INTO supplier_performance
                                (po_id, supplier_id, sku, quantity, unit_cost, po_raised_at, goods_received_at, lead_time_hours)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (po_id, row.get('supplier_id'), row.get('sku'), row.get('quantity'),
                              row.get('unit_cost'), po_raised_at[po_id], row.get('timestamp'), lead_hours))
                        conn.commit()
                        print(f"📊 [Group B] Supplier lead time for {po_id}: {lead_hours}h")
                    except Exception as e:
                        print("❌ Failed to log supplier performance:", e)

            sku = row.get('sku')
            warehouse_id = row.get('warehouse_id') or row.get('destination_warehouse_id')
            if not sku or not warehouse_id:
                continue

            on_hand, in_transit = get_watch_row(cursor, sku, warehouse_id)
            timestamp = row.get('event_timestamp') or row.get('timestamp')

            if topic == 'stock-movement-topic':
                on_hand += int(row.get('quantity', 0) or 0)

            elif topic == 'transfers-topic':
                status = row.get('status')
                qty = int(row.get('quantity', 0) or 0)
                dest = row.get('destination_warehouse_id')
                if status == 'IN_TRANSIT' and warehouse_id == dest:
                    in_transit += qty
                elif status == 'RECEIVED' and warehouse_id == dest:
                    in_transit -= qty

            upsert_watch(cursor, sku, warehouse_id, on_hand, in_transit, timestamp)
            conn.commit()

            available_estimate = on_hand + in_transit
            alert_fired = available_estimate < REORDER_THRESHOLD
            if alert_fired:
                cursor.execute("""
                    INSERT INTO reorder_alerts (sku, warehouse_id, available_estimate, triggered_at)
                    VALUES (%s, %s, %s, %s)
                """, (sku, warehouse_id, available_estimate, timestamp))
                conn.commit()
                print(f"🚨 [Group B] Reorder alert: {sku}@{warehouse_id} available≈{available_estimate}")

            try:
                events_collection.insert_one({
                    "topic": topic,
                    "raw_event": row,
                    "sku": sku,
                    "warehouse_id": warehouse_id,
                    "on_hand_estimate": on_hand,
                    "in_transit": in_transit,
                    "available_estimate": available_estimate,
                    "reorder_alert_fired": alert_fired,
                    "processed_at": timestamp,
                })
                saved += 1
                print(f"💾 [{sku}@{warehouse_id}] available≈{available_estimate} → saved #{saved}")
            except Exception as e:
                print("❌ Failed to log event to MongoDB:", e)

    except KeyboardInterrupt:
        print(f"\n🛑 Stopped by user. Consumed {saved} events total, saved to MongoDB.")
        print("Events consumed per topic:")
        for topic, n in sorted(topic_counts.items()):
            print(f"  {topic}: {n}")
    finally:
        cursor.close()
        conn.close()
        mongo_client.close()
        consumer.close()


if __name__ == '__main__':
    main()