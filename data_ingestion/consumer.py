from kafka import KafkaConsumer
import json

KAFKA_BROKER = 9092
topic = "ssa"

consumer = KafkaConsumer(
    bootstap_servers=[KAFKA_BROKER],
    group_id="",retry_backoff="",
    value_desrializer=lambda m: json.loads(m.decode)
)

# function to consume to cassandra



# function to conume to s3

