from kafka import KafkaConsumer
import json

KAFKA_BROKER = 9092

consumer = KafkaConsumer(
    bootstap_servers=[KAFKA_BROKER]
)

# function to consume to cassandra



# function to conume to s3

