from kafka import KafkaProducer
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any
import uuid
import sys
import os
import asyncio
import pandas as pd
import concurrent.futures

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

        FILE_PATH = "/app/data_utils/restaurants_cleaned.csv"
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
        """Function to produce events asynchronously"""
        topic = self.topics[topic_key]
        partition_key = self._get_partition_key(topic, event)
        logger.info(f"Sending event to topic {topic} with key {partition_key} and event {event}")

        # Use a thread pool to handle the synchronous Kafka producer
        loop = asyncio.get_event_loop()
        try:
            future = loop.run_in_executor(
                None,  # Default thread pool
                lambda: self.producer.send(
                    topic,
                    key=partition_key,
                    value=event,
                    timestamp_ms=int(datetime.now().timestamp() * 1000),
                ).get(timeout=10)
            )
            await future
            logger.info(f"Event sent to {topic} with key {partition_key}")
        except Exception as e:
            logger.error(f"Error sending event to {topic}: {str(e)}")
            raise

    async def producing(self, events: int = 1):
        try:
            while True:
                shared_event_id = str(uuid.uuid4())
                merchant = self.generator.generate_merchant_event(shared_event_id)
                await self.send_event("merchants", merchant["merchant_id"], merchant
                )
                order_id, order_event = self.generator.generate_order_event(shared_event_id)
                await self.send_event("orders", order_id, order_event)

                courier = self.generator.generate_courier_event(shared_event_id)
                await self.send_event("couriers", courier["courier_id"], courier)

                review = self.generator.generate_review_event(shared_event_id)
                await self.send_event("reviews", review["review_id"], review)

                time.sleep(1.0 / events)

        except KeyboardInterrupt:
            logger.info("Stopping event production...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    producer = DeliveryEventProducer(bootstrap_servers="kafka-broker-1:9092")
    asyncio.run(producer.producing(events=10))
    print("✅ Kafka is ready!")