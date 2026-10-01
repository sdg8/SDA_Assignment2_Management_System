"""
Consumer Group C: Adjustment & Batch Consumer

Subscribes to ONE topic:
    - adjustments-topic  (returns, damage write-offs, batch/serial assignment)

Every adjustment is written to MongoDB in full (including free-text
reasons and batch/serial numbers, which don't fit a fixed schema well).
Quantity-affecting adjustment types are ALSO logged to a MySQL table so
there is a queryable audit trail of shrinkage and returns.

DEFECT CLUSTER ALERTING:
Maintains a rolling count per (sku, reason) in MongoDB
(adjustment_reason_counts). Once the same reason has been logged
DEFECT_THRESHOLD times or more for a SKU, a DEFECT_CLUSTER alert is
raised and persisted to adjustment_alerts -- this is what surfaces a
SKU that keeps coming back damaged or gets returned for the same
reason repeatedly, rather than treating every adjustment as an
isolated, unrelated event.

NOTE on the same design gap flagged in stock_position_consumer.py:
this consumer does not push its quantity-affecting adjustments back
into Group A's on_hand number. In this architecture Group A only reads
stock-movement-topic for physical quantity, so a DAMAGE_WRITE_OFF here
will not automatically appear in Group A's stock_position table. If
that matters for your use case, the real fix is either to also
subscribe Group A to adjustments-topic, or have this consumer publish
a synthetic correction event onto stock-movement-topic.

Usage:
    python adjustment_batch_consumer.py
"""

import json
import sys

from kafka import KafkaConsumer, errors
import mysql.connector
from pymongo import MongoClient

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'adjustments-topic'

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': 'root',
    'database': 'stockdb'
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = 'stockdb'

QUANTITY_AFFECTING_ADJUSTMENTS = {'RETURN_TO_STOCK', 'DAMAGE_WRITE_OFF'}
DEFECT_THRESHOLD = 3  # same (sku, reason) seen this many times -> alert


def ensure_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS adjustment_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            adjustment_id VARCHAR(20),
            product_id VARCHAR(20),
            sku VARCHAR(30),
            warehouse_id VARCHAR(20),
            adjustment_type VARCHAR(30),
            quantity INT,
            reason VARCHAR(255),
            batch_no VARCHAR(30),
            serial_no VARCHAR(30),
            timestamp VARCHAR(50)
        )
    """)


def main():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='adjustment-batch-group'
        )
        print(f"✅ Connected to Kafka, subscribed to '{TOPIC}'")
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available")
        sys.exit(1)

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        ensure_table(cursor)
        conn.commit()
        print("✅ Connected to MySQL, ensured 'adjustment_log' table exists")
    except Exception as e:
        print("❌ Error connecting to MySQL:", e)
        sys.exit(1)

    try:
        mongo_client = MongoClient(MONGO_URI)
        adjustments_collection = mongo_client[MONGO_DB]['adjustments']
        reason_counts_collection = mongo_client[MONGO_DB]['adjustment_reason_counts']
        alerts_collection = mongo_client[MONGO_DB]['adjustment_alerts']
        print(f"✅ Connected → {mongo_client[MONGO_DB].name}.{adjustments_collection.name}")
    except Exception as e:
        print("❌ Error connecting to MongoDB:", e)
        sys.exit(1)

    print(f"📥 [Group C] Listening on {TOPIC} ...")
    saved = 0
    topic_counts = {}
    try:
        for message in consumer:
            row = message.value
            topic_counts[message.topic] = topic_counts.get(message.topic, 0) + 1
            print(f"[adjustments-topic] {row}")

            # full document always goes to Mongo, whatever the type
            try:
                adjustments_collection.insert_one(dict(row))
                saved += 1
                sku = row.get('sku', '?')
                adj_type = row.get('adjustment_type', '?')
                print(f"💾 [{sku}] {adj_type} → saved #{saved}")
            except Exception as e:
                print("❌ Failed to write to MongoDB:", e)

            # quantity-affecting types also get a queryable MySQL row
            if row.get('adjustment_type') in QUANTITY_AFFECTING_ADJUSTMENTS:
                try:
                    cursor.execute("""
                        INSERT INTO adjustment_log
                            (adjustment_id, product_id, sku, warehouse_id, adjustment_type,
                             quantity, reason, batch_no, serial_no, timestamp)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        row.get('adjustment_id'), row.get('product_id'), row.get('sku'),
                        row.get('warehouse_id'), row.get('adjustment_type'), row.get('quantity'),
                        row.get('reason'), row.get('batch_no'), row.get('serial_no'), row.get('timestamp'),
                    ))
                    conn.commit()
                except Exception as e:
                    print("❌ Failed to write to MySQL:", e)

                # rolling aggregate: how many times has this SKU had this
                # exact reason logged? Once it crosses DEFECT_THRESHOLD,
                # raise and persist a DEFECT_CLUSTER alert.
                sku = row.get('sku')
                reason = row.get('reason')
                if sku and reason:
                    try:
                        doc_id = f"{sku}|{reason}"
                        reason_counts_collection.update_one(
                            {"_id": doc_id},
                            {"$inc": {"count": 1},
                             "$set": {"sku": sku, "reason": reason,
                                      "adjustment_type": row.get('adjustment_type'),
                                      "last_seen": row.get('timestamp')}},
                            upsert=True
                        )
                        agg = reason_counts_collection.find_one({"_id": doc_id})
                        if agg and agg.get("count", 0) >= DEFECT_THRESHOLD:
                            alerts_collection.insert_one({
                                "alert_type": "DEFECT_CLUSTER",
                                "severity": "HIGH",
                                "raised_by": "adjustment_batch_consumer",
                                "sku": sku,
                                "reason": reason,
                                "adjustment_type": row.get('adjustment_type'),
                                "occurrence_count": agg["count"],
                                "source_event": row,
                                "alert_at": row.get('timestamp'),
                                "status": "open",
                            })
                            print(f"🚨 [Group C] HIGH ALERT: DEFECT_CLUSTER — {sku} flagged "
                                  f"'{reason}' {agg['count']}x — halt sales pending QC check")
                    except Exception as e:
                        print("❌ Failed to update defect cluster tracking:", e)

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