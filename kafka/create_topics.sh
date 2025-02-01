#!/bin/bash

# Define the Kafka topics to create
TOPICS=(
    "food-delivery.orders.raw"
    "food-delivery.couriers.raw"
    "food-delivery.merchants.raw"
    "food-delivery.reviews.raw"
)

# Run the Kafka topic creation commands inside the Kafka Docker container
for TOPIC in "${TOPICS[@]}"
do
    docker exec -it kafka \
        kafka-topics.sh --create \
        --topic $TOPIC \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1
done