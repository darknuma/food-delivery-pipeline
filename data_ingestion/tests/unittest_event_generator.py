import unittest
from unittest.mock import patch, MagicMock
import uuid
from datetime import datetime
from decimal import Decimal
import pandas as pd
from ..data_utils.event_generator import DeliveryEventGenerator
from ..data_utils.data_schema import (
     ServiceType, PaymentStatus, PaymentMethod, OrderStatus)

class TestDeliveryEventGenerator(unittest.TestCase):

    def setUp(self):
        # Sample mock merchant data
        self.merchant_data = pd.DataFrame({
            "Name": ["Merchant A", "Merchant B", "Merchant C"],
            "Address": ["Address A", "Address B", "Address C"]
        })
        self.generator = DeliveryEventGenerator(self.merchant_data)

        self.generator.menu_items = {}
        for merchant_id in self.generator.merchants:
            self.generator.menu_items[merchant_id] = [{
                "item_id": str(uuid.uuid4()),
                "name": "Test Item",
                "price": Decimal("10.00"),
                "category": "Test Category"
            }]
        
        # Initialize customer addresses if not already done
        self.generator.customer_addresses = ["Test Address"]

    def test_generate_order_event(self):
        # Test order event generation
        event_id = str(uuid.uuid4())
        order_id, order_event = self.generator.generate_order_event(event_id)

        # Assert that the order event contains the correct fields
        self.assertEqual(order_event['event_id'], event_id)
        self.assertIsInstance(order_event['order_id'], str)
        self.assertIsInstance(order_event['event_timestamp'], datetime)
        self.assertIn('merchant_id', order_event)
        self.assertIn('customer_id', order_event)
        self.assertIn('items', order_event)
        self.assertGreater(order_event['total_amount'], 0)  # total amount should be greater than zero

    def test_generate_courier_event(self):
        # Test courier event generation
        event_id = str(uuid.uuid4())
        courier_event = self.generator.generate_courier_event(event_id)

        # Assert that the courier event contains the correct fields
        self.assertEqual(courier_event['event_id'], event_id)
        self.assertIsInstance(courier_event['event_timestamp'], datetime)
        self.assertIn('courier_id', courier_event)
        self.assertIn('order_id', courier_event)  # order_id can be None if no active orders exist
        self.assertIn('delivery_status', courier_event)
        self.assertIn('vehicle_type', courier_event)

    def test_generate_merchant_event(self):
        # Test merchant event generation
        event_id = str(uuid.uuid4())
        merchant_event = self.generator.generate_merchant_event(event_id)

        # Assert that the merchant event contains the correct fields
        self.assertEqual(merchant_event['event_id'], event_id)
        self.assertIsInstance(merchant_event['event_timestamp'], datetime)
        self.assertIn('merchant_id', merchant_event)
        self.assertIn('order_id', merchant_event)  # order_id can be None if no active orders exist
        self.assertIn('is_open', merchant_event)

    def test_generate_review_event(self):
        # Test review event generation
        event_id = str(uuid.uuid4())
        
        # Generate order event first to ensure there's an active order
        order_id, _ = self.generator.generate_order_event(event_id)
        
        # Generate a review event for the active order
        review_event = self.generator.generate_review_event(event_id)

        # Assert that the review event contains the correct fields
        self.assertIsInstance(review_event['review_id'], str)
        self.assertEqual(review_event['order_id'], order_id)
        self.assertIn('customer_id', review_event)
        self.assertIn('courier_rating', review_event)
        self.assertIn('merchant_rating', review_event)
        self.assertIn('delivery_rating', review_event)

    def test_generate_order_event_no_orders(self):
        # If no orders have been generated, test that the review event is None
        generator_no_orders = DeliveryEventGenerator(self.merchant_data)
        event_id = str(uuid.uuid4())
        
        review_event = generator_no_orders.generate_review_event(event_id)
        
        # Test that the review event is None if no active orders
        self.assertIsNone(review_event)

    def test_load_merchants(self):
        # Test if merchants are loaded correctly
        self.assertEqual(len(self.generator.merchants), 3)  # 3 merchants in mock data
        
        # Get any merchant from the dictionary
        any_merchant = next(iter(self.generator.merchants.values()))
        # Check if merchant_id exists in the merchant's data dictionary
        self.assertIn("merchant_id", any_merchant)

    @patch('data_ingestion.data_utils.event_generator.fake')
    def test_generate_order_event_with_mocked_faker(self, mock_faker):
        # Get the first merchant's ID
        merchant_id = next(iter(self.generator.merchants.keys()))
        
        # Set base coordinates
        base_lat = 10.0
        base_lon = 20.0
        
        # Update the merchant's location
        self.generator.merchants[merchant_id]["location"].latitude = base_lat
        self.generator.merchants[merchant_id]["location"].longitude = base_lon
        
        # Create mock menu items for the merchant
        mock_menu_items = [{
            "item_id": str(uuid.uuid4()),
            "name": "Test Item",
            "price": Decimal("10.00"),
            "category": "Test Category"
        }]
        
        self.generator.menu_items[merchant_id] = mock_menu_items
        
        # Mock all the necessary random calls
        with patch('random.choice', side_effect=[
            merchant_id,  # for merchant selection
            mock_menu_items[0],  # for menu item selection
            "Test Address",  # for customer address
            ServiceType.FOOD_DELIVERY,  # for service type
            OrderStatus.READY_FOR_PICKUP,  # for order status
            PaymentMethod.CARD,  # for payment method
            PaymentStatus.COMPLETED  # for payment status
        ]), patch('random.randint', return_value=1), patch('random.uniform', return_value=0.0):
            
            event_id = str(uuid.uuid4())
            order_id, order_event = self.generator.generate_order_event(event_id)

            # Check that values are within the expected range
            self.assertEqual(order_event['delivery_location']['latitude'], base_lat)
            self.assertEqual(order_event['delivery_location']['longitude'], base_lon)

    def test_generate_courier_event_with_no_active_couriers(self):
        # Test that a new courier is created if there are no active couriers
        self.generator.active_couriers = set()  # Make sure no couriers are active
        event_id = str(uuid.uuid4())
        courier_event = self.generator.generate_courier_event(event_id)
        
        self.assertIsNotNone(courier_event['courier_id'])  # New courier should be generated

    def test_generate_merchant_event_no_active_orders(self):
        # Test that merchant event is generated even with no active orders
        self.generator.active_orders = {}
        event_id = str(uuid.uuid4())
        merchant_event = self.generator.generate_merchant_event(event_id)

        # Assert merchant event is still generated with valid fields
        self.assertIn('merchant_id', merchant_event)

if __name__ == '__main__':
    unittest.main()
