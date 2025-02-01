from kafka import KafkaProducer
import logging, json, time
from datetime import datetime
from typing import Dict, Any
import uuid
import sys
import os
import asyncio
import pandas as pd 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_utils.event_generator import DeliveryEventGenerator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeliveryEventProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
            key_serializer=lambda x: x.encode("utf-8"),
            retries=3,
            acks="all",
        )
       
        FILE_PATH = "./data_utils/restaurants_cleaned.csv"
        merchant_data = pd.read_csv(FILE_PATH)  

        self.generator = DeliveryEventGenerator(merchant_data)
        

        self.topics = {
            "orders": "food-delivery-orders-raw",
            "couriers": "food-delivery-couriers-raw",
            "merchants": "food-delivery-merchants-raw",
            "reviews": "food-delivery-reviews-raw",
        }

    def _get_partition_key(self, topic: str, event: Dict[str, Any]) -> str:
        """Determine partition key based on topic and event type"""
        key_mapping = {
            "food-delivery-orders-raw": event.get("order_id"),
            "food-delivery-merchants-raw": event.get("merchant_id"),
            "food-delivery-couriers-raw": event.get("courier_id"),
            "food-delivery-reviews-raw": event.get("review_id"),
        }
        return key_mapping.get(topic, event.get("event_id"))

    async def send_event(self, topic_key: str, key: str, event: Dict[str, Any]):
        """Function to production"""
        topic = self.topics[topic_key]
        key = self._get_partition_key(topic, event)
        try:
            self.producer.send(
                topic,
                key=key,
                value=event,
                timestamp_ms=int(datetime.now().timestamp() * 1000),
            ).get(timeout=10)

        except Exception as e:
            logger.error(f"Error sending event to {topic}: {str(e)}")
            raise

    def producing(self, events: int = 1):
        try:
            while True:
                shared_event_id = str(uuid.uuid4())

                merchant = self.generator.generate_merchant_event(shared_event_id)
                self.send_event(
                    self.topics["merchants"], merchant["merchant_id"], merchant
                )

                order = self.generator.generate_order_event(shared_event_id)
                self.send_event(self.topics["orders"], order["order_id"], order)

                courier = self.generator.generate_courier_event(shared_event_id)
                self.send_event(self.topics["couriers"], courier["courier_id"], courier)

                review = self.generator.generate_review_event(shared_event_id)
                self.send_event(self.topics["review"], courier["review_id"], review)

                time.sleep(1.0 / events)

        except KeyboardInterrupt:
            logger.info("Stopping event production...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    while True:
        try:
            producer = DeliveryEventProducer(bootstrap_servers="kafka-broker-1:9092")
            asyncio.run(producer.producing(events=2))
            print("✅ Kafka is ready!")
            break
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(5)

    
