
# List of topics to create
TOPICS=(
    "food-delivery.orders.raw"
    "food-delivery.couriers.raw"
    "food-delivery.merchants.raw"
    "food-delivery.reviews.raw"
)

# Create topics
for TOPIC in "${TOPICS[@]}"; do
    $KAFKA_HOME/bin/kafka-topics.sh --create \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --replication-factor 1 \
        --partitions 3 \
        --topic $TOPIC
    echo "Created topic: $TOPIC"
done