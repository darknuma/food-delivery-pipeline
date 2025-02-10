import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
import json
import uuid
import asyncio
from kafka import KafkaProducer
from data_ingestion.data_utils.event_generator import DeliveryEventGenerator
from data_ingestion.src.delivery_producer import DeliveryEventProducer

class TestDeliveryEventGenerator(unittest.TestCase):
    def setUp(self):
        merchant_data = pd.DataFrame({
            "Name": ["Pizza Place", "Burger Joint"],
            "Address": ["123 Main St", "456 Elm St"]
        })
        self.generator = DeliveryEventGenerator(merchant_data)
        self.shared_event_id = str(uuid.uuid4())

    def test_generate_order_event(self):
        order_id, order_event = self.generator.generate_order_event(self.shared_event_id)
        self.assertIsInstance(order_event, dict)
        self.assertEqual(order_event["event_id"], self.shared_event_id)

    def test_generate_courier_event(self):
        courier_event = self.generator.generate_courier_event(self.shared_event_id)
        self.assertIsInstance(courier_event, dict)
        self.assertEqual(courier_event["event_id"], self.shared_event_id)

    def test_generate_merchant_event(self):
        merchant_event = self.generator.generate_merchant_event(self.shared_event_id)
        self.assertIsInstance(merchant_event, dict)
        self.assertEqual(merchant_event["event_id"], self.shared_event_id)

    def test_generate_review_event(self):
        review_event = self.generator.generate_review_event(self.shared_event_id)
        self.assertTrue(review_event is None or isinstance(review_event, dict))


class TestDeliveryEventProducer(unittest.TestCase):
    @patch("data_ingestion.src.delivery_producer.KafkaProducer")
    def setUp(self, MockKafkaProducer):
        self.mock_producer = MockKafkaProducer()
        self.producer = DeliveryEventProducer(bootstrap_servers="localhost:9092")
        self.producer.producer = self.mock_producer  # Injecting mock producer

    def test_get_partition_key(self):
        event = {"order_id": "1234"}
        key = self.producer._get_partition_key("food-delivery-orders-raw", event)
        self.assertEqual(key, "1234")

    @patch("data_ingestion.src.delivery_producer.DeliveryEventGenerator")
    @patch("data_ingestion.src.delivery_producer.DeliveryEventProducer.send_event", new_callable=AsyncMock)
    def test_producing(self, mock_send_event, MockEventGenerator):
        mock_generator = MockEventGenerator.return_value
        mock_generator.generate_order_event.return_value = ("order123", {"event_id": "event123"})
        self.producer.generator = mock_generator

        # Run the async method inside an event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.producer.send_event("orders", "order123", {"event_id": "event123"}))

        mock_send_event.assert_called_once()


# class TestKafkaIntegration(unittest.TestCase):
#     """
#     This is an optional test to verify Kafka integration.
#     Requires a running Kafka instance.
#     """
#     def setUp(self):
#         self.bootstrap_servers = "kafka-broker-1:9092"
#         self.producer = KafkaProducer(
#             bootstrap_servers=self.bootstrap_servers,
#             value_serializer=lambda x: json.dumps(x).encode("utf-8"),
#             key_serializer=lambda x: x.encode("utf-8")
#         )
#         self.topic = "test-kafka-topic"

#     def test_send_event_to_kafka(self):
#         key = "test-key"
#         value = {"message": "Test Kafka Event"}
#         future = self.producer.send(self.topic, key=key, value=value)
#         record_metadata = future.get(timeout=10)

#         self.assertEqual(record_metadata.topic, self.topic)

#     def tearDown(self):
#         self.producer.close()


if __name__ == "__main__":
    unittest.main()