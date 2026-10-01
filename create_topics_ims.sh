#!/bin/bash
# Creates the Kafka topics for the Inventory Management System (IMS)
# streaming pipeline described in the project report:
#   - purchase-orders-topic   (procurement system, lifecycle/status)
#   - sales-orders-topic      (sales/order system, lifecycle/status)
#   - stock-movement-topic   (warehouse scanners, physical stock deltas)
#   - transfers-topic         (inter-warehouse transfers, lifecycle/status)
#   - adjustments-topic       (returns, write-offs, batch/serial assignment)
#
# Run once after your Docker containers are up.
# Adjust the container name (sda-kafka-1) if yours differs — check with `docker ps`.

KAFKA_CONTAINER=ims-kafka-1

for TOPIC in purchase-orders-topic sales-orders-topic stock-movement-topic transfers-topic adjustments-topic; do
  echo "Creating topic: $TOPIC"
  docker exec -it $KAFKA_CONTAINER kafka-topics.sh --create \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 \
    --replication-factor 3 \
    --partitions 3
done

echo ""
echo "All topics:"
docker exec -it $KAFKA_CONTAINER kafka-topics.sh --list --bootstrap-server localhost:9092
