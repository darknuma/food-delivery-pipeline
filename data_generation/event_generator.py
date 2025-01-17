import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple
import json
from faker import Faker
from data_schema import (
    OrderEvent, CourierEvent, MerchantEvent, Location,
    OrderItem, OrderStatus, DeliveryStatus, ServiceType,
    PaymentStatus, PaymentMethod, OrderReview
)

fake = Faker() 

class DeliveryEventGenerator:
    def __init__(self, merchant_data: pd.DataFrame):
        self.merchant_data =  merchant_data
        self.active_couriers = set()
        self.active_orders = {}
        self.merchants = {}
        self.active_couriers = set()
        self.active_orders = {}
        self.customer_ids = {}
        self.order_reviews = {}
        self._load_merchants()  
        self.menu_items = self._load_menu_items()
        self.customer_addresses = df["Address"].to_list()

    def _load_merchants(self) -> Dict[str, dict]:
        """Load merchant data with locations"""
        merchants = {}
        for _, row in self.merchant_data.iterrows():
            merchant_id = str(uuid.uuid4())
            merchants[merchant_id] = {
                "id": merchant_id,
                "name": row["Name"],
                "location": Location(
                    latitude=fake.latitude(),
                    longitude=fake.longitude(),
                    address=row["Address"]
                )
            }
        self.merchants = merchants        
    
    def _load_menu_items(self) -> Dict[str, List[Dict]]:
         """Load menu items for each merchant"""
         return {
            merchant_id: [
                {
                    "item_id": f"item_{i}",
                    "name": f"Menu Item {i}",
                    "price": Decimal(random.randint(1000, 5000))
                }
                for i in range(1, 6)  
            ]
            for merchant_id in self.merchants.keys()
        }

    def generate_order_event(self, event_id:str) -> Tuple[str, dict]:
        """Generate a new order event"""
        merchant_id = random.choice(list(self.merchants.keys()))
        merchant = self.merchants[merchant_id]
        
        items = []
        for _ in range(random.randint(1, 4)):
            menu_item = random.choice(self.menu_items[merchant_id])
            quantity = random.randint(1, 3)
            items.append(
                OrderItem(
                    item_id=menu_item["item_id"],
                    name=menu_item["name"],
                    quantity=quantity,
                    unit_price=menu_item["price"],
                    total_price=menu_item["price"] * quantity
                )
            )

        total_amount = sum(item.total_price for item in items)
        delivery_fee = Decimal("1000.00")
        
        delivery_location = Location(
            latitude=merchant["location"].latitude + random.uniform(-0.5, 0.5),  
            longitude=merchant["location"].longitude + random.uniform(-0.5, 0.5), 
            address=random.choice(self.customer_addresses)
        )

        order_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        
        order_event = OrderEvent(
            event_id=event_id,
            event_timestamp=datetime.now(),
            order_id=order_id,
            merchant_id=merchant_id,
            customer_id=customer_id,
            service_type=random.choice(list(ServiceType)),
            order_status=random.choice(list(OrderStatus)),
            delivery_location=delivery_location,
            items=items,
            delivery_fee=delivery_fee,
            total_amount=total_amount,
            payment_method=random.choice(list(PaymentMethod)),
            payment_status=random.choice(list(PaymentStatus)),
            estimated_delivery_time=datetime.now() + timedelta(minutes=45)
        )
            

        self.active_orders[order_id] = order_event
        self.customer_ids[order_id] = customer_id 
        return order_id, order_event.model_dump()

    def generate_courier_event(self, event_id:str) -> dict:
        """Generate a courier event"""
          # Ensure at least one active courier exists
        if not self.active_couriers or (random.random() < 0.3 and len(self.active_couriers) < 50):
            courier_id = str(uuid.uuid4())
            self.active_couriers.add(courier_id)
        else:
            courier_id = random.choice(list(self.active_couriers))

        courier_address = random.choice(self.merchant_data["Address"].to_list())

        location = Location(
            latitude=fake.latitude() + Decimal(random.uniform(-0.1, 0.1)),
            longitude=fake.longitude() + Decimal(random.uniform(-0.1, 0.1)),
            address=courier_address
        )
        
        if self.active_orders:
            random_order_id = random.choice(list(self.active_orders.keys()))
            merchant_id = self.active_orders[random_order_id].merchant_id
        else:
            merchant_id = random.choice(list(self.merchants.keys()))

        courier_event = CourierEvent(
            event_id=event_id,
            event_timestamp=datetime.now(),
            mechant_id=merchant_id, 
            courier_id=courier_id,
            order_id=random.choice(list(self.active_orders.keys())) if self.active_orders else None,
            delivery_status=random.choice(list(DeliveryStatus)),
            current_location=location,
            battery_level=random.uniform(0.3, 1.0),
            is_online=True,
            vehicle_type=random.choice(["Bicycle", "Motorcycle", "Van"]),
            
        )

        return courier_event.model_dump()

    def generate_merchant_event(self, event_id:str) -> dict:
        """Generate a merchant status event"""
         # If there are active orders, pick a random order's merchant_id
        if self.active_orders:
            random_order_id = random.choice(list(self.active_orders.keys()))
            merchant_id = self.active_orders[random_order_id].merchant_id
        else:
            # Fallback to random merchant if no active orders exist
            merchant_id = random.choice(list(self.merchants.keys()))
        # try:
        merchant_event = MerchantEvent(
            event_id=event_id,
            event_timestamp=datetime.now(),
            merchant_id=merchant_id,
            order_id=random.choice(list(self.active_orders.keys())) if self.active_orders else None,
            is_open=True,
            average_preparation_time=random.randint(15, 45),
            current_load=random.randint(1, 10)
            
        )
        return merchant_event.model_dump()
            
    def generate_review_event(self, event_id:str) -> dict:
        """Generate a customer feedback""" 
      
        if not self.active_orders:
            return None  # Or handle the no-orders case differently
            
        # Select a random active order
        order_id = random.choice(list(self.active_orders.keys()))
        order = self.active_orders[order_id]  
       

        feedback_event = OrderReview(
            review_id=str(uuid.uuid4()),
            event_id=event_id, 
            order_id=order_id,
            customer_id=order.customer_id,
            courier_rating=random.randint(1,5),
            merchant_rating=random.randint(1,5),
            delivery_rating=random.randint(1,5),
            feedback_timestamp=datetime.now(),
            comments=fake.text(max_nb_chars=200)
        )
        

        self.order_reviews[order_id] = feedback_event

        return feedback_event.model_dump()



if __name__ == "__main__":
    FILE_PATH = "restaurants_cleaned.csv"
    df = pd.read_csv(FILE_PATH) 
    generator = DeliveryEventGenerator(df)
    shared_event_id = str(uuid.uuid4())
    
    order_id, order_event = generator.generate_order_event(shared_event_id)
    courier_event = generator.generate_courier_event(shared_event_id)
    merchant_event = generator.generate_merchant_event(shared_event_id)
    review_event = generator.generate_review_event(shared_event_id)
    
    print("Order Event:", json.dumps(order_event, indent=2, default=str))
    print("Courier Event:", json.dumps(courier_event, indent=2, default=str))
    print("Merchant Event:", json.dumps(merchant_event, indent=2, default=str))
    print("Review Event:", json.dumps(review_event, indent=2,default=str))