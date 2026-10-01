"""
Consumer Group A: Stock-Position Consumer

Subscribes to THREE topics, matching the architecture diagram:
    - purchase-orders-topic  (to track stock ON ORDER, not yet received)
    - sales-orders-topic     (to track stock COMMITTED to a confirmed order)
    - stock-movement-topic   (the only source of ON HAND physical quantity)

Writes to BOTH stores, each for a different reason:
    MySQL   -> stock_position: the CURRENT state per (sku, warehouse_id),
               upserted in place. Good for "what is the stock right now"
               queries, but it does not keep history -- each update
               overwrites the previous one.
    MongoDB -> stock_position_events: a raw, append-only log of every
               single event this consumer processed and what it
               computed from it. This is what lets you later ask
               "show me the full history of this SKU's stock position"
               or replay/audit exactly how a number was derived.

This consumer computes three separate numbers per (sku, warehouse_id),
not just one:

    on_hand              -- physically in the warehouse right now
                             (derived ONLY from stock-movement-topic,
                             never from purchase-orders or sales-orders,
                             to avoid double-counting the same unit twice)
    on_order              -- quantity raised on a PO but not yet received
    committed             -- quantity reserved against a sales order but
                             not yet issued
    available_to_promise  -- on_hand - committed + on_order

IMPORTANT DESIGN NOTE (read before treating on_hand as complete):
adjustments-topic (returns, damage write-offs) is owned by Consumer
Group C in this architecture, NOT by this consumer. That means
RETURN_TO_STOCK and DAMAGE_WRITE_OFF events do not currently adjust
on_hand here. In a production system you would either (a) also
subscribe this consumer to adjustments-topic, or (b) have Group C
publish a correction event onto stock-movement-topic so this consumer
picks it up naturally. This file deliberately follows the diagram's
strict topic ownership as given; the gap is called out rather than
silently papered over.

Usage:
    python stock_position_consumer.py
"""

import json
import sys

from kafka import KafkaConsumer, errors
import mysql.connector
from pymongo import MongoClient

KAFKA_BROKER = 'localhost:9092'
TOPICS = ['purchase-orders-topic', 'sales-orders-topic', 'stock-movement-topic']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': 'root',
    'database': 'stockdb'
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = 'stockdb'

LOW_STOCK_THRESHOLD = 15


def ensure_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_position (
            sku VARCHAR(30),
            warehouse_id VARCHAR(20),
            on_hand INT DEFAULT 0,
            on_order INT DEFAULT 0,
            committed INT DEFAULT 0,
            available_to_promise INT DEFAULT 0,
            stock_out_status VARCHAR(20),
            last_updated VARCHAR(50),
            PRIMARY KEY (sku, warehouse_id)
        )
    """)


def get_row(cursor, sku, warehouse_id):
    cursor.execute(
        "SELECT on_hand, on_order, committed FROM stock_position WHERE sku=%s AND warehouse_id=%s",
        (sku, warehouse_id)
    )
    row = cursor.fetchone()
    return row if row else (0, 0, 0)


def compute_status(on_hand):
    if on_hand <= 0:
        return "OUT_OF_STOCK"
    elif on_hand < LOW_STOCK_THRESHOLD:
        return "LOW_STOCK"
    return "IN_STOCK"


def upsert(cursor, sku, warehouse_id, on_hand, on_order, committed, timestamp):
    atp = on_hand - committed + on_order
    status = compute_status(on_hand)
    cursor.execute("""
        INSERT INTO stock_position (sku, warehouse_id, on_hand, on_order, committed, available_to_promise, stock_out_status, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            on_hand = VALUES(on_hand),
            on_order = VALUES(on_order),
            committed = VALUES(committed),
            available_to_promise = VALUES(available_to_promise),
            stock_out_status = VALUES(stock_out_status),
            last_updated = VALUES(last_updated)
    """, (sku, warehouse_id, on_hand, on_order, committed, atp, status, timestamp))
    return status


def main():
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='stock-position-group'
        )
        print(f"✅ Connected to Kafka, subscribed to {TOPICS}")
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available")
        sys.exit(1)

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        ensure_table(cursor)
        conn.commit()
        print("✅ Connected to MySQL, ensured 'stock_position' table exists")
    except Exception as e:
        print("❌ Error connecting to MySQL:", e)
        sys.exit(1)

    try:
        mongo_client = MongoClient(MONGO_URI)
        events_collection = mongo_client[MONGO_DB]['stock_position_events']
        alerts_collection = mongo_client[MONGO_DB]['stock_alerts']
        print(f"✅ Connected → {mongo_client[MONGO_DB].name}.{events_collection.name}")
    except Exception as e:
        print("❌ Error connecting to MongoDB:", e)
        sys.exit(1)

    print(f"📥 [Group A] Listening on {TOPICS} ...")
    saved = 0
    topic_counts = {}
    try:
        for message in consumer:
            topic = message.topic
            row = message.value
            sku = row.get('sku')
            warehouse_id = row.get('warehouse_id')
            if not sku or not warehouse_id:
                continue

            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            on_hand, on_order, committed = get_row(cursor, sku, warehouse_id)
            prev_status = compute_status(on_hand)
            timestamp = row.get('event_timestamp') or row.get('timestamp')

            if topic == 'stock-movement-topic':
                on_hand += int(row.get('quantity', 0) or 0)

            elif topic == 'purchase-orders-topic':
                status = row.get('status')
                qty = int(row.get('quantity', 0) or 0)
                if status == 'PO_RAISED':
                    on_order += qty
                elif status == 'GOODS_RECEIVED':
                    on_order -= qty  # now physically arriving via stock-movement-topic instead

            elif topic == 'sales-orders-topic':
                status = row.get('status')
                qty = int(row.get('quantity', 0) or 0)
                if status == 'STOCK_RESERVED':
                    committed += qty
                elif status in ('STOCK_ISSUED', 'CANCELLED'):
                    committed -= qty  # either fulfilled (on_hand already dropped) or released

            print(f"[{topic}] {sku}@{warehouse_id}: on_hand={on_hand} on_order={on_order} committed={committed}")

            try:
                new_status = upsert(cursor, sku, warehouse_id, on_hand, on_order, committed, timestamp)
                conn.commit()

                try:
                    events_collection.insert_one({
                        "topic": topic,
                        "raw_event": row,
                        "sku": sku,
                        "warehouse_id": warehouse_id,
                        "on_hand": on_hand,
                        "on_order": on_order,
                        "committed": committed,
                        "available_to_promise": on_hand - committed + on_order,
                        "stock_out_status": new_status,
                        "processed_at": timestamp,
                    })
                    saved += 1
                    print(f"💾 [{sku}@{warehouse_id}] on_hand={on_hand} → saved #{saved}")
                except Exception as e:
                    print("❌ Failed to log event to MongoDB:", e)

                if new_status in ("LOW_STOCK", "OUT_OF_STOCK") and new_status != prev_status:
                    severity = "HIGH" if new_status == "OUT_OF_STOCK" else "MEDIUM"
                    try:
                        alerts_collection.insert_one({
                            "alert_type": new_status,
                            "severity": severity,
                            "raised_by": "stock_position_consumer",
                            "sku": sku,
                            "warehouse_id": warehouse_id,
                            "on_hand": on_hand,
                            "available_to_promise": on_hand - committed + on_order,
                            "triggering_topic": topic,
                            "source_event": row,
                            "alert_at": timestamp,
                            "status": "open",
                        })
                    except Exception as e:
                        print("❌ Failed to save alert to MongoDB:", e)
                    print(f"🚨 [Group A] {severity} ALERT: {sku}@{warehouse_id} is {new_status} (on_hand={on_hand})")
            except Exception as e:
                print("❌ Failed to update stock_position:", e)

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