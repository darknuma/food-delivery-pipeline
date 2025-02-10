import unittest
from unittest.mock import patch, MagicMock
import json
import asyncio
from datetime import datetime
from ..src.delivery_producer import DeliveryEventProducer 


class TestDeliveryEventProducer(unittest.TestCase):

    @patch("data_ingestion.src.delivery_producer.KafkaProducer")
    def setUp(self, MockKafkaProducer):
        """Set up the test with a mocked KafkaProducer"""
        self.mock_producer = MagicMock()
        MockKafkaProducer.return_value = self.mock_producer
        
        self.producer = DeliveryEventProducer(bootstrap_servers="mocked_kafka_broker")

    def test_get_partition_key(self):
        """Test the _get_partition_key function"""
        event = {"order_id": "12345", "merchant_id": "merchant_1", "courier_id": "courier_1"}
        
        # Test with the 'orders' topic
        partition_key = self.producer._get_partition_key("orders", event)
        self.assertEqual(partition_key, "12345")
        
        # Test with the 'merchants' topic
        partition_key = self.producer._get_partition_key("merchants", event)
        self.assertEqual(partition_key, "merchant_1")
        
        # Test with the 'couriers' topic
        partition_key = self.producer._get_partition_key("couriers", event)
        self.assertEqual(partition_key, "courier_1")

        # Test with a fallback to 'event_id'
        partition_key = self.producer._get_partition_key("unknown_topic", event)
        self.assertEqual(partition_key, event.get("event_id"))

    @patch("asyncio.get_event_loop")
    @patch("data_ingestion.src.delivery_producer.KafkaProducer.send")
    def test_send_event(self, mock_send, mock_get_event_loop):
        """Test that the send_event method works correctly"""
        mock_send.return_value.get.return_value = None  # Mock the Kafka send success
        
        # Event data
        event = {
            "order_id": "12345",
            "merchant_id": "merchant_1",
            "courier_id": "courier_1"
        }
        
        # Simulate the loop and event sending
        loop = MagicMock()
        mock_get_event_loop.return_value = loop

        topic_key = "orders"
        key = "12345"
        
        # Call the async send_event function
        asyncio.run(self.producer.send_event(topic_key, key, event))

        # Assert that the send function was called with the correct parameters
        mock_send.assert_called_with(
            "food-delivery-orders-raw",
            key="12345",
            value=event,
            timestamp_ms=int(datetime.now().timestamp() * 1000),
        )

    @patch("data_ingestion.src.delivery_producer.DeliveryEventGenerator.generate_order_event")
    @patch("data_ingestion.src.delivery_producer.DeliveryEventGenerator.generate_courier_event")
    @patch("data_ingestion.src.delivery_producer.DeliveryEventGenerator.generate_merchant_event")
    @patch("data_ingestion.src.delivery_producer.DeliveryEventGenerator.generate_review_event")
    @patch("data_ingestion.src.delivery_producer.DeliveryEventProducer.send_event")
    @patch("time.sleep", return_value=None)  # Mock sleep to avoid delay in tests
    def test_producing(self, mock_sleep, mock_send_review, mock_send_merchant, mock_send_courier, mock_send_order, mock_generate_order_event, mock_generate_courier_event, mock_generate_merchant_event, mock_generate_review_event):
        """Test the producing function to ensure proper event generation"""

        # Setup the mock methods to return expected events
        shared_event_id = "test-event-id"
        mock_generate_merchant_event.return_value = {"merchant_id": "merchant_1"}
        mock_generate_order_event.return_value = ("order_1", {"order_id": "12345"})
        mock_generate_courier_event.return_value = {"courier_id": "courier_1"}
        mock_generate_review_event.return_value = {"review_id": "review_1"}

        # Call the producing function (for one event)
        asyncio.run(self.producer.producing(events=1))

        # Ensure send_event is called for each event type
        mock_send_merchant.assert_called_once_with("merchants", "merchant_1", {"merchant_id": "merchant_1"})
        mock_send_order.assert_called_once_with("orders", "12345", {"order_id": "12345"})
        mock_send_courier.assert_called_once_with("couriers", "courier_1", {"courier_id": "courier_1"})
        mock_send_review.assert_called_once_with("reviews", "review_1", {"review_id": "review_1"})

        # Verify sleep is called (to control event frequency)
        mock_sleep.assert_called_with(0.1)


if __name__ == "__main__":
    unittest.main()
