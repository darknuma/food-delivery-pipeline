from kafka import KafkaProducer 
import json
import time 
from data_generation.event_generator  import (
    DeliveryEventGenerator
)

KAFKA_BROKER=9092
TOPIC="food-delivery"
BATCH_SIZE = 1000
TOTAL_SIZE_GB = 1  # Total data size to generate (in GB)
DEFAULT_RECORD_SIZE = 1024  # Approximate size per record in bytes
TARGET_RECORD_COUNT = int((TOTAL_SIZE_GB * 1024 * 1024 * 1024) / DEFAULT_RECORD_SIZE)


# Producer setup for Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data to Kafka
def send_to_kafka(data, topic=TOPIC):
    producer.send(topic, value=data)
    producer.flush()

# Main data generation loop
for _ in range(TARGET_RECORD_COUNT // BATCH_SIZE):
    order_batch = []
    merchant_batch = []
    feedback_batch = []
    courier_batch = []

    for _ in range(BATCH_SIZE):
        order = DeliveryEventGenerator.generate_order_event()
        order_batch.append(order)
        
        # Send order to Kafka
        send_to_kafka(order, topic="orders")
        
        merchant = DeliveryEventGenerator.generate_merchant_event()
        merchant['order_id'] = order['order_id']
        merchant_batch.append(merchant)
        
        # Send merchant to Kafka
        send_to_kafka(merchant, topic="merchant_events")
        
        feedback =DeliveryEventGenerator.generate_review_event(order['order_id'])
        feedback_batch.append(feedback)
        
        # Send feedback to Kafka
        send_to_kafka(feedback, topic="customer_feedback")
        
        courier = DeliveryEventGenerator.generate_courier_event()
        courier_batch.append(courier)
        
        # Send courier to Kafka
        send_to_kafka(courier, topic="couriers")
        

    
    # Sleep for rate control (if needed)
    time.sleep(1)

print("Data generation completed.")
producer.close()