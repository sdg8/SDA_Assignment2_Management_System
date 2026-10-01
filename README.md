# Streaming Data Analytics — Assignment 2
### Live E-commerce Inventory Management System → Kafka → MongoDB / MySQL

**Industry (Assignment 1):** E-commerce
**Data source (Assignment 1):** Inventory Management System (IMS)

This repo implements the exact architecture submitted in Assignment 1: **3 producers → 5 topics → 3 consumer groups → MySQL + MongoDB → dashboards.**

Instead of replaying a static CSV, the three `*_producer_live.py` scripts run
**forever**, generating new events with Python + Faker until stopped with
Ctrl+C. Purchase orders, sales orders, and warehouse transfers are modelled
as real lifecycle state machines — each one is created, then progresses
through its stages one tick at a time, exactly the way a real PO or order
would move through a system over time. The WMS & Scanner Producer is a
hybrid: it also *consumes* `purchase-orders-topic` and `sales-orders-topic`
so that a `GOODS_RECEIVED` or `STOCK_ISSUED` status immediately triggers a
correctly cross-referenced physical stock movement — no two runs produce
the same data, and every stock movement can be traced back to the real PO,
order, or transfer that caused it.

---

## Architecture

```
1. DATA SOURCES            2. PRODUCERS                 3. KAFKA CLUSTER              4. CONSUMER GROUPS              5. STORAGE
────────────────           ────────────                 ────────────────              ──────────────────              ─────────
Purchase / Procurement ──▶ Procurement Producer ──────── purchase-orders-topic ──┬──▶ Group A: Stock-Position ───────┐
                                                                                  │    Consumer                       │
Sales / Order System ────▶ Sales / POS Producer ──────── sales-orders-topic ────┼──▶ (also read by Group B)          ├─▶ MySQL
                                                                                  │                                    │   stock_position, reorder_alerts,
Warehouse Management  ┐                              ┌── stock-movement-topic ──┼──▶ Group B: Reorder & Supplier ───┤   supplier_performance,
Stock Transfers        ├──▶ WMS & Scanner Producer ───┼── transfers-topic ───────┘    Consumer                       │   adjustment_log
Returns & Adjustments  ┘    (reacts to PO/order       └── adjustments-topic ─────────▶ Group C: Adjustment & Batch ──┘
Batch / Serial Records      events + generates its                                    Consumer                       └─▶ MongoDB
                             own transfers/counts)                                                                        stock_position_events, reorder_events,
                                                                                                                            adjustments, stock_alerts,
        ZooKeeper ensemble: cluster metadata, leader election, failure detection                                          adjustment_reason_counts, adjustment_alerts
```

### Producer → topic ownership

| Producer | Topics it owns | Source events |
|---|---|---|
| **Procurement Producer** | `purchase-orders-topic` | PO lifecycle: `PO_RAISED` → `PO_APPROVED` → `GOODS_RECEIVED` → `PO_CLOSED` |
| **Sales / POS Producer** | `sales-orders-topic` | Order lifecycle: `ORDER_PLACED` → `STOCK_RESERVED` → `STOCK_ISSUED` (or `CANCELLED`, ~12% of orders) |
| **WMS & Scanner Producer** | `stock-movement-topic`, `transfers-topic`, `adjustments-topic` | Physical stock receipts/issues/cycle counts, warehouse transfers, returns/damage/batch assignment |

The WMS & Scanner Producer does two jobs at once: it **consumes**
`purchase-orders-topic` and `sales-orders-topic` and reacts the instant it
sees `GOODS_RECEIVED` or `STOCK_ISSUED` by publishing a matching stock
movement referencing the real PO/order ID — and it **independently**
generates transfers, cycle counts, and adjustments, since those don't
depend on the other two producers.

### Consumer → topic subscription

| Consumer (Group) | Subscribes to | Writes to |
|---|---|---|
| **Stock-Position Consumer** (Group A) | `purchase-orders-topic`, `sales-orders-topic`, `stock-movement-topic` | MySQL `stock_position` (on_hand, on_order, committed, available_to_promise) · Mongo `stock_position_events`, `stock_alerts` |
| **Reorder & Supplier Consumer** (Group B) | `purchase-orders-topic`, `stock-movement-topic`, `transfers-topic` | MySQL `reorder_watch`, `reorder_alerts`, `supplier_performance` · Mongo `reorder_events` |
| **Adjustment & Batch Consumer** (Group C) | `adjustments-topic` | MySQL `adjustment_log` · Mongo `adjustments`, `adjustment_reason_counts`, `adjustment_alerts` |

**Message keys:** `po_id` for purchase orders, `order_id` for sales orders,
`transfer_id` for transfers, and `sku:warehouse_id` for stock movements and
adjustments. With the default partitioner (`hash(key) % partitions`), every
event for the same item at the same warehouse lands in the same partition
and is consumed in the order it occurred — a `STOCK_ISSUE` can never be
applied before the `STOCK_RECEIPT` that preceded it.

**A note on consumer groups:** each consumer uses its own `group_id`
(`stock-position-group`, `reorder-supplier-group`, `adjustment-batch-group`).
In Kafka, members of *one* group share the partitions of the topics they
subscribe to. `purchase-orders-topic` and `stock-movement-topic` are each
read independently by **two** different groups (A and B) — this only works
correctly because Group A and Group B are separate consumer groups, so each
gets its own full copy of the topic rather than competing for the same
messages. The "Consumer Groups" box in the diagram is the logical
stream-processing layer, not a single Kafka group id.

**A documented design gap, not hidden:** `adjustments-topic` (returns,
damage write-offs) is owned only by Group C, so Group A's `on_hand` figure
does not currently reflect returns or damage after the fact. A production
fix would be either to also subscribe Group A to `adjustments-topic`, or
have Group C publish a correction event onto `stock-movement-topic`.

---

## Files

| File | Purpose |
|---|---|
| `procurement_producer_live.py` | Runs forever — generates and progresses purchase orders |
| `sales_pos_producer_live.py` | Runs forever — generates and progresses sales orders |
| `wms_scanner_producer_live.py` | Runs forever — reacts to PO/order events + generates transfers, cycle counts, adjustments |
| `stock_position_consumer.py` | Group A — computes on-hand/on-order/committed stock position |
| `reorder_supplier_consumer.py` | Group B — reorder-point alerts + supplier lead-time performance |
| `adjustment_batch_consumer.py` | Group C — returns/damage/batch logging + defect-cluster alerting |
| `generate_ims_sample_data.py` | Generates a fixed, cross-referenced sample dataset (894 rows) for offline testing |
| `ims_sample_data/*.csv` | Pre-generated sample data: `purchase_orders.csv`, `sales_orders.csv`, `stock_movements.csv`, `transfers.csv`, `adjustments.csv` |
| `create_topics_ims.sh` | Creates the five Kafka topics |
| `requirements.txt` | `kafka-python`, `mysql-connector-python`, `pymongo`, `faker` |

---

## How to run

**1. Start Kafka and create topics**

```bash
docker-compose up -d
chmod +x create_topics_ims.sh
bash create_topics_ims.sh              # edit KAFKA_CONTAINER inside if your container name differs
```

**2. Python environment**

```bash
python -m venv venv
venv\Scripts\activate                  # Windows
# source venv/bin/activate             # macOS / Linux
pip install -r requirements.txt
```

**3. Consumers** (3 terminals) — start these first so no early events are missed

```bash
python stock_position_consumer.py
python reorder_supplier_consumer.py
python adjustment_batch_consumer.py
```

Each connects to MongoDB Atlas — set your own connection string inside each
file before running:
```python
MONGO_URI = "mongodb+srv://<username>:<password>@<cluster-url>/?retryWrites=true&w=majority"
```

**4. Producers** (3 more terminals) — start these close together in time

```bash
python procurement_producer_live.py --rate 1.5
python sales_pos_producer_live.py --rate 0.8
python wms_scanner_producer_live.py
```

Starting all three close together matters: `wms_scanner_producer_live.py`
uses `auto_offset_reset='latest'` on its internal consumer, so it only
reacts to PO/order events published *after it starts* — if Procurement runs
alone for a while first, those early `GOODS_RECEIVED` events will be missed.

Let it run for a minute or two, then `Ctrl+C` each terminal. Every producer
and consumer prints a per-topic breakdown on exit, e.g.:

```
🛑 Stopped. Published 234 events total.
Events sent per topic:
  adjustments-topic: 35
  stock-movement-topic: 136
  transfers-topic: 63
```


### Producer flags

| Flag | Applies to | Meaning |
|---|---|---|
| `--rate 1.5` | Procurement Producer | average seconds between events (default 1.5) |
| `--rate 0.8` | Sales / POS Producer | average seconds between events (default 0.8) |

`wms_scanner_producer_live.py` has no rate flag — its pace is driven by how
fast it reacts to incoming PO/order events plus a fixed average delay
between its own independently generated events.

---

## Alert rules (Box 4: "flag anomalies")

| Alert | Raised by | Trigger | Severity |
|---|---|---|---|
| `LOW_STOCK` | Stock-Position Consumer | `on_hand` falls below 15 units (real transition only, not every event) | MEDIUM |
| `OUT_OF_STOCK` | Stock-Position Consumer | `on_hand` reaches 0 (real transition only) | HIGH |
| Reorder alert | Reorder & Supplier Consumer | `on_hand_estimate + in_transit` falls below the reorder threshold (20 units) | — |
| `DEFECT_CLUSTER` | Adjustment & Batch Consumer | same SKU + reason logged 3+ times (`RETURN_TO_STOCK` / `DAMAGE_WRITE_OFF`) | HIGH |

These map directly to the real-time decisions identified in Assignment 1 —
catching a stock-out before it causes an oversold order, triggering a
reorder before a shelf actually empties, and halting sales of a SKU that
keeps coming back damaged for the same reason while the pattern is still
fresh enough to investigate.
