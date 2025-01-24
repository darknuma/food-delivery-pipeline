from kafka import KafkaProducer
from data_utils.event_generator import DeliveryEventGenerator
import logging, json, time
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeliveryEventProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
            key_serializer=lambda x: x.encode("utf-8"),
            enable_idempotence=True,
            retries=3,
            acks=10,
        )

        self.generator = DeliveryEventGenerator()

        self.topics = {
            "orders": "food-delivery.orders.raw",
            "couriers": "food-delivery.couriers.raw",
            "merchants": "food-delivery.merchants.raw",
            "review": "food-delivery.review.raw",
        }

    def _get_partition_key(self, topic: str, event: Dict[str, Any]) -> str:
        """Determine partition key based on topic and event type"""
        key_mapping = {
            "food-delivery.orders.raw": event.get("order_id"),
            "food-delivery.merchants.raw": event.get("merchant_id"),
            "food-delivery.couriers.raw": event.get("couriers_id"),
            "food-delivery.review.raw": event.get("review_id"),
        }
        return key_mapping.get(topic, event.get("event_id"))

    async def send_event(self, topic_key: str, key: str, event: Dict[str, Any]):
        """Function to production"""
        topic = self.topics[topic_key]
        partition_key = self._get_partition_key(topic, event)
        try:
            self.producer.send(
                topic,
                key=partition_key,
                value=event,
                timestamp_ms=int(datetime.now().timestamp() * 1000),
            ).get(timeout=10)

        except Exception as e:
            logger.error(f"Error sending event to {topic}: {str(e)}")
            raise

    def producing(self, events: int = 1):
        try:
            while True:

                merchant = self.generator.generate_merchant_event()
                self.send_event(
                    self.topics["merchants"], merchant["merchane_id"], merchant
                )

                order = self.generator.generate_order_event()
                self.send_event(self.topics["order"], order["order_id", order])

                courier = self.generator.generate_courier_event()
                self.send_event(self.topics["courier"], courier["courier_id"], courier)

                review = self.generator.generate_review_event()
                self.send_event(self.topics["review"], courier["review_id"], review)

                time.sleep(1.0 / events)

        except KeyboardInterrupt:
            logger.info("Stopping event production...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    producer = DeliveryEventProducer(bootstrap_servers="localhost:9092")
    producer.producing(events=2)
